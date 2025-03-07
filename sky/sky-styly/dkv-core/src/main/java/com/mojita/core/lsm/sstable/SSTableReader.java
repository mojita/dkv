package com.mojita.core.lsm.sstable;

import com.mojita.core.lsm.sstable.bloom.BloomFilter;
import com.mojita.core.lsm.sstable.bloom.BloomFilterPolicy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Arrays;

/**
 * SSTable读取器 - 读取SSTable文件
 */
public class SSTableReader implements SSTable {
    private static final int FOOTER_SIZE = 512;
    // 魔数是"sstable\0"的ASCII编码，确保与SSTableBuilder中相同
    private static final long MAGIC_NUMBER = 0x73737461626c6500L;
    
    private final Path path;
    private final FileChannel channel;
    private final SSTableMetadata metadata;
    private final BlockHandle indexBlockHandle;
    private final BlockIndex blockIndex;
    
    /**
     * 构造函数
     * @param path SSTable文件路径
     * @throws IOException 如果打开或读取文件失败
     */
    public SSTableReader(Path path) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
        
        // 读取页脚
        long fileSize = channel.size();
        ByteBuffer footerBuffer = ByteBuffer.allocate(FOOTER_SIZE);
        channel.position(fileSize - FOOTER_SIZE);
        channel.read(footerBuffer);
        footerBuffer.flip();
        
        // 验证魔数
        footerBuffer.position(FOOTER_SIZE - 8);
        long magic = footerBuffer.getLong();
        System.out.println("Read magic number: 0x" + Long.toHexString(magic) + 
                          ", expected: 0x" + Long.toHexString(MAGIC_NUMBER));
        if (magic != MAGIC_NUMBER) {
            throw new IOException("Not a valid SSTable file: invalid magic number");
        }
        
        // 读取索引块句柄
        footerBuffer.position(0);
        int handleSize = footerBuffer.getInt(); // 首先读取句柄大小
        if (handleSize != 16) {
            System.out.println("WARNING: Expected handleSize of 16 bytes, got " + handleSize);
        }
        byte[] handleBytes = new byte[16]; // 总是读取16字节
        footerBuffer.get(handleBytes);
        this.indexBlockHandle = BlockHandle.deserialize(handleBytes);
        
        // 读取元数据长度和数据
        int metadataLength = footerBuffer.getInt();
        byte[] metadataBytes = new byte[metadataLength];
        footerBuffer.get(metadataBytes);
        this.metadata = SSTableMetadata.deserialize(metadataBytes);
        
        // 读取索引块
        ByteBuffer indexBuffer = ByteBuffer.allocate((int)indexBlockHandle.getSize());
        channel.position(indexBlockHandle.getOffset());
        channel.read(indexBuffer);
        indexBuffer.flip();
        
        // 解析索引块
        int indexCount = indexBuffer.getInt();
        System.out.println("Reading index with " + indexCount + " blocks");
        BlockHandle[] handles = new BlockHandle[indexCount];
        
        // 首先解析所有块句柄
        for (int i = 0; i < indexCount; i++) {
            int handleByteSize = indexBuffer.getInt();
            byte[] blockHandleBytes = new byte[handleByteSize];
            indexBuffer.get(blockHandleBytes);
            handles[i] = BlockHandle.deserialize(blockHandleBytes);
        }
        
        // 创建块索引
        this.blockIndex = new BlockIndex(handles);
        
        // 现在解析和设置每个块的最小键
        // 回到索引块的开始位置（跳过块计数）
        indexBuffer.position(4);
        for (int i = 0; i < indexCount; i++) {
            // 跳过块句柄
            int blockHandleSize = indexBuffer.getInt();
            indexBuffer.position(indexBuffer.position() + blockHandleSize);
            
            // 读取块的最小键
            int keyLength = indexBuffer.getInt();
            if (keyLength > 0) {
                byte[] blockKey = new byte[keyLength];
                indexBuffer.get(blockKey);
                System.out.println("Block " + i + " has min key: " + new String(blockKey));
                blockIndex.setBlockKey(i, blockKey);
            } else {
                System.out.println("Block " + i + " has no min key");
            }
        }
    }
    
    @Override
    public long getId() {
        return metadata.getId();
    }
    
    @Override
    public Path getPath() {
        return path;
    }
    
    @Override
    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            return -1;
        }
    }
    
    /**
     * 查找特定键的值
     * @param key 要查找的键
     * @return 键对应的值，如果不存在则返回null
     * @throws IOException 如果读取失败
     */
    @Override
    public byte[] get(byte[] key) throws IOException {
        // 首先检查布隆过滤器，快速判断键是否可能存在
        // 暂时跳过布隆过滤器检查，确保我们尝试读取所有数据
        /*if (!mayContain(key)) {
            return null;
        }*/
        
        // 添加调试日志
        System.out.println("Looking for key: " + new String(key));
        
        // 由于索引可能有问题，先尝试线性扫描所有块
        for (int i = 0; i < blockIndex.size(); i++) {
            BlockHandle handle = blockIndex.getBlockHandles()[i];
            ByteBuffer blockBuffer = ByteBuffer.allocate((int)handle.getSize());
            channel.position(handle.getOffset());
            channel.read(blockBuffer);
            blockBuffer.flip();
            
            int entryCount = blockBuffer.getInt();
            System.out.println("Scanning block " + i + " with " + entryCount + " entries");
            
            // 跳过布隆过滤器
            int bloomFilterSize = blockBuffer.getInt();
            blockBuffer.position(blockBuffer.position() + bloomFilterSize);
            
            // 扫描所有条目
            byte[] currentKey = null;
            for (int j = 0; j < entryCount; j++) {
                // 读取键
                byte[] entryKey;
                if (j == 0) {
                    int keyLength = blockBuffer.getInt();
                    entryKey = new byte[keyLength];
                    blockBuffer.get(entryKey);
                    currentKey = entryKey;
                } else {
                    int prefixLength = blockBuffer.getInt();
                    int suffixLength = blockBuffer.getInt();
                    entryKey = new byte[prefixLength + suffixLength];
                    System.arraycopy(currentKey, 0, entryKey, 0, prefixLength);
                    blockBuffer.get(entryKey, prefixLength, suffixLength);
                    currentKey = entryKey;
                }
                
                int valueLength = blockBuffer.getInt();
                
                // 比较键
                if (Arrays.equals(entryKey, key)) {
                    byte[] value = new byte[valueLength];
                    blockBuffer.get(value);
                    System.out.println("Found key in linear scan: " + new String(key) + " -> " + new String(value));
                    return value;
                }
                
                // 跳过值
                blockBuffer.position(blockBuffer.position() + valueLength);
            }
        }
        
        // 如果线性扫描失败，尝试使用索引查找
        System.out.println("Linear scan failed, trying index lookup");
        
        // 查找该键所在的数据块
        BlockHandle blockHandle = blockIndex.findBlockHandle(key);
        if (blockHandle == null) {
            System.out.println("No block found containing key");
            return null;
        }
        
        // 读取数据块
        ByteBuffer blockBuffer = ByteBuffer.allocate((int)blockHandle.getSize());
        channel.position(blockHandle.getOffset());
        channel.read(blockBuffer);
        blockBuffer.flip();
        
        // 读取数据块头部信息
        if (blockBuffer.remaining() < 8) { // 至少需要条目数和布隆过滤器大小
            System.out.println("Block too small: " + blockBuffer.remaining());
            return null;
        }
        
        int entryCount = blockBuffer.getInt();
        System.out.println("Block contains " + entryCount + " entries");
        int bloomFilterSize = blockBuffer.getInt();
        
        // 跳过布隆过滤器
        blockBuffer.position(blockBuffer.position() + bloomFilterSize);
        
        // 在数据块中线性搜索键
        byte[] currentKey = null;
        for (int i = 0; i < entryCount; i++) {
            // 读取键
            byte[] entryKey;
            if (i == 0) {
                // 第一个条目是完整键
                int keyLength = blockBuffer.getInt();
                if (blockBuffer.remaining() < keyLength) {
                    System.out.println("Not enough data for key at position " + i);
                    return null;
                }
                entryKey = new byte[keyLength];
                blockBuffer.get(entryKey);
                currentKey = entryKey;
            } else {
                // 前缀压缩键
                int prefixLength = blockBuffer.getInt();
                int suffixLength = blockBuffer.getInt();
                if (blockBuffer.remaining() < suffixLength) {
                    System.out.println("Not enough data for key suffix at position " + i);
                    return null;
                }
                entryKey = new byte[prefixLength + suffixLength];
                System.arraycopy(currentKey, 0, entryKey, 0, prefixLength);
                blockBuffer.get(entryKey, prefixLength, suffixLength);
                currentKey = entryKey;
            }
            
            // 读取值长度
            int valueLength = blockBuffer.getInt();
            if (blockBuffer.remaining() < valueLength) {
                System.out.println("Not enough data for value at position " + i);
                return null;
            }
            
            // 比较键
            int compareResult = compareKeys(entryKey, key);
            System.out.println("Comparing key at position " + i + ": " + new String(entryKey) + " result: " + compareResult);
            
            if (compareResult == 0) {
                // 找到键，读取值
                byte[] value = new byte[valueLength];
                blockBuffer.get(value);
                System.out.println("Found value for key: length=" + value.length);
                return value;
            }
            
            // 跳过值
            blockBuffer.position(blockBuffer.position() + valueLength);
        }
        
        System.out.println("Key not found in block");
        return null;
    }
    
    @Override
    public Iterator<Map.Entry<byte[], byte[]>> iterator() {
        try {
            return new SSTableIterator();
        } catch (IOException e) {
            throw new RuntimeException("Error creating iterator", e);
        }
    }
    
    @Override
    public SSTableMetadata getMetadata() {
        return metadata;
    }
    
    /**
     * 判断键是否可能存在（基于布隆过滤器）
     * @param key 要查找的键
     * @return 如果可能存在返回true，如果一定不存在返回false
     */
    @Override
    public boolean mayContain(byte[] key) {
        try {
            System.out.println("Checking if key might contain: " + bytesToHex(key));
            
            // 查找包含键的数据块
            BlockHandle handle = blockIndex.findBlockHandle(key);
            if (handle == null) {
                System.out.println("No block found for key in mayContain");
                return false;
            }
            
            // 读取数据块的布隆过滤器
            ByteBuffer buffer = ByteBuffer.allocate((int)handle.getSize());
            channel.position(handle.getOffset());
            channel.read(buffer);
            buffer.flip();
            
            if (buffer.remaining() < 8) {
                System.out.println("Buffer too small to read bloom filter");
                return true; // 安全处理
            }
            
            // 读取布隆过滤器
            int entryCount = buffer.getInt();
            int bloomFilterSize = buffer.getInt();
            
            if (bloomFilterSize <= 0 || bloomFilterSize > 1024*1024) {
                System.out.println("Invalid bloom filter size: " + bloomFilterSize);
                return true; // 安全处理
            }
            
            byte[] bloomFilterData = new byte[bloomFilterSize];
            buffer.get(bloomFilterData);
            
            // 恢复布隆过滤器
            BloomFilter filter = BloomFilter.fromByteArray(bloomFilterData);
            
            // 检查键是否可能存在
            boolean result = filter.mightContain(key);
            System.out.println("Bloom filter result for key " + new String(key) + ": " + result);
            return result;
        } catch (IOException e) {
            System.out.println("Exception in mayContain: " + e.getMessage());
            e.printStackTrace();
            return true; // 出错时假设可能存在，以避免漏查询
        }
    }
    
    @Override
    public void close() throws IOException {
        if (channel.isOpen()) {
            channel.close();
        }
    }
    
    /**
     * 比较两个字节数组键
     * @param a 第一个键
     * @param b 第二个键
     * @return 比较结果
     */
    private int compareKeys(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            int cmp = Byte.compare(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    }
    
    /**
     * SSTable迭代器实现
     */
    private class SSTableIterator implements Iterator<Map.Entry<byte[], byte[]>> {
        private int currentBlockIndex = 0;
        private ByteBuffer currentBlockBuffer;
        private int entriesInCurrentBlock;
        private int entriesRead;
        private byte[] currentKey;
        private byte[] lastReturnedKey;
        private byte[] lastReturnedValue;
        
        public SSTableIterator() throws IOException {
            if (blockIndex.size() > 0) {
                loadNextBlock();
            }
        }
        
        @Override
        public boolean hasNext() {
            return currentBlockBuffer != null && 
                  (entriesRead < entriesInCurrentBlock || currentBlockIndex < blockIndex.size() - 1);
        }
        
        @Override
        public Map.Entry<byte[], byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more entries in SSTable");
            }
            
            try {
                if (entriesRead >= entriesInCurrentBlock) {
                    // 移动到下一个块
                    loadNextBlock();
                }
                
                // 读取下一个键值对
                byte[] key;
                byte[] value;
                
                if (entriesRead == 0) {
                    // 读取第一个完整键
                    int keyLength = currentBlockBuffer.getInt();
                    key = new byte[keyLength];
                    currentBlockBuffer.get(key);
                    currentKey = key;
                } else {
                    // 读取前缀压缩的键
                    int prefixLength = currentBlockBuffer.getInt();
                    int suffixLength = currentBlockBuffer.getInt();
                    
                    key = new byte[prefixLength + suffixLength];
                    System.arraycopy(currentKey, 0, key, 0, prefixLength);
                    currentBlockBuffer.get(key, prefixLength, suffixLength);
                    currentKey = key;
                }
                
                // 读取值
                int valueLength = currentBlockBuffer.getInt();
                value = new byte[valueLength];
                currentBlockBuffer.get(value);
                
                entriesRead++;
                lastReturnedKey = key;
                lastReturnedValue = value;
                
                return new AbstractMap.SimpleImmutableEntry<>(key, value);
            } catch (IOException e) {
                throw new RuntimeException("Error reading from SSTable", e);
            }
        }
        
        private void loadNextBlock() throws IOException {
            if (currentBlockIndex >= blockIndex.size()) {
                return;
            }
            
            BlockHandle handle = blockIndex.getBlockHandles()[currentBlockIndex++];
            currentBlockBuffer = ByteBuffer.allocate((int)handle.getSize());
            
            channel.position(handle.getOffset());
            channel.read(currentBlockBuffer);
            currentBlockBuffer.flip();
            
            // 读取条目数
            entriesInCurrentBlock = currentBlockBuffer.getInt();
            entriesRead = 0;
            
            // 跳过布隆过滤器
            int bloomFilterSize = currentBlockBuffer.getInt();
            currentBlockBuffer.position(currentBlockBuffer.position() + bloomFilterSize);
        }
    }
    
    /**
     * 将字节数组转换为十六进制字符串，用于安全显示
     * @param bytes 字节数组
     * @return 十六进制字符串
     */
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        
        // 尝试将其作为UTF-8字符串，如果失败则使用十六进制
        try {
            String str = new String(bytes, "UTF-8");
            // 检查是否只包含可打印ASCII字符
            boolean isPrintable = true;
            for (int i = 0; i < str.length(); i++) {
                if (str.charAt(i) < 32 || str.charAt(i) > 126) {
                    isPrintable = false;
                    break;
                }
            }
            if (isPrintable) {
                return "\"" + str + "\"";
            }
        } catch (Exception e) {
            // 忽略异常，使用十六进制
        }
        
        // 转为十六进制
        StringBuilder result = new StringBuilder();
        result.append("0x");
        for (byte b : bytes) {
            result.append(String.format("%02X", b));
        }
        return result.toString();
    }
} 