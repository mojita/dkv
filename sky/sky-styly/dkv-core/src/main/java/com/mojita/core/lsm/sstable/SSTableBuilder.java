package com.mojita.core.lsm.sstable;

import com.mojita.core.lsm.sstable.bloom.BloomFilter;
import com.mojita.core.lsm.sstable.bloom.BloomFilterPolicy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * SSTable构建器 - 创建SSTable文件
 */
public class SSTableBuilder implements AutoCloseable {
    private static final int DEFAULT_BLOCK_SIZE = 4096;  // 4KB
    private static final int FOOTER_SIZE = 512;         // 增大页脚大小，容纳更多元数据
    
    private final FileChannel channel;                  // 输出文件通道
    private final Path path;                            // 文件路径
    private final long tableId;                         // SSTable标识
    private final int blockSize;                        // 数据块大小
    private final BloomFilterPolicy bloomFilterPolicy;  // 布隆过滤器策略
    
    private final List<BlockHandle> indexBlocks;        // 索引块列表
    private final Map<byte[], byte[]> pendingEntries;   // 待写入的键值对
    private final ByteBuffer dataBlockBuffer;           // 数据块缓冲区
    
    private long currentOffset;                        // 当前文件偏移量
    private byte[] smallestKey;                        // 最小键
    private byte[] largestKey;                         // 最大键
    private long recordCount;                          // 记录数
    
    private final Comparator<byte[]> comparator;       // 键比较器

    // 成员变量用于存储每个数据块的最小键
    private List<byte[]> blockMinKeysForIndex;

    /**
     * 构造函数
     * @param path SSTable文件路径
     * @param tableId SSTable ID
     * @param blockSize 数据块大小
     * @param bloomFilterPolicy 布隆过滤器策略
     * @throws IOException 如果创建文件失败
     */
    public SSTableBuilder(Path path, long tableId, int blockSize, BloomFilterPolicy bloomFilterPolicy) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        this.path = path;
        this.tableId = tableId;
        this.blockSize = blockSize > 0 ? blockSize : DEFAULT_BLOCK_SIZE;
        this.bloomFilterPolicy = bloomFilterPolicy;
        
        this.indexBlocks = new ArrayList<>();
        this.pendingEntries = new TreeMap<>((a, b) -> {
            // 字节数组比较逻辑
            int minLen = Math.min(a.length, b.length);
            for (int i = 0; i < minLen; i++) {
                int cmp = Byte.compare(a[i], b[i]);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(a.length, b.length);
        });
        this.comparator = (a, b) -> {
            // 字节数组比较逻辑
            int minLen = Math.min(a.length, b.length);
            for (int i = 0; i < minLen; i++) {
                int cmp = Byte.compare(a[i], b[i]);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(a.length, b.length);
        };
        
        this.dataBlockBuffer = ByteBuffer.allocateDirect(this.blockSize * 4);
        this.currentOffset = 0;
        this.recordCount = 0;
        
        this.blockMinKeysForIndex = new ArrayList<>();
    }
    
    /**
     * 简化构造函数 - 使用默认值
     * @param path SSTable文件路径
     * @param tableId SSTable ID
     * @throws IOException 如果创建文件失败
     */
    public SSTableBuilder(Path path, long tableId) throws IOException {
        this(path, tableId, DEFAULT_BLOCK_SIZE, new BloomFilterPolicy.DefaultPolicy(10));
    }

    /**
     * 添加键值对
     * @param key 键
     * @param value 值
     * @throws IOException 如果写入失败
     */
    public void add(byte[] key, byte[] value) throws IOException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }
        
        // 更新统计信息
        if (smallestKey == null || comparator.compare(key, smallestKey) < 0) {
            smallestKey = key.clone();
        }
        if (largestKey == null || comparator.compare(key, largestKey) > 0) {
            largestKey = key.clone();
        }
        recordCount++;
        
        // 添加到待处理条目
        pendingEntries.put(key.clone(), value.clone());
        
        // 如果积累的数据大小超过块大小，则刷新
        int estimatedSize = estimateBlockSize();
        if (estimatedSize >= blockSize) {
            flushDataBlock();
        }
    }

    /**
     * 估计当前块大小
     * @return 估计的大小（字节）
     */
    private int estimateBlockSize() {
        int size = 0;
        for (Map.Entry<byte[], byte[]> entry : pendingEntries.entrySet()) {
            // 每个条目: keyLength(4) + key + valueLength(4) + value
            size += 8 + entry.getKey().length + entry.getValue().length;
        }
        return size;
    }

    /**
     * 刷新数据块到文件
     * @return 数据块句柄
     * @throws IOException 如果写入失败
     */
    private BlockHandle flushDataBlock() throws IOException {
        if (pendingEntries.isEmpty()) {
            return null;
        }
        
        System.out.println("Flushing data block with " + pendingEntries.size() + " entries");
        
        // 保存当前块的最小键（用于索引）
        byte[] minKey = null;
        for (byte[] key : pendingEntries.keySet()) {
            if (minKey == null || comparator.compare(key, minKey) < 0) {
                minKey = key.clone();
            }
            break; // 只需要第一个键（TreeMap已排序）
        }
        System.out.println("Data block min key: " + (minKey != null ? new String(minKey) : "null"));
        
        // 保存此块的最小键供索引使用
        if (minKey != null) {
            blockMinKeysForIndex.add(minKey.clone());
        }
        
        // 创建布隆过滤器
        BloomFilter bloomFilter = null;
        byte[] bloomFilterData = new byte[0];
        
        if (bloomFilterPolicy != null) {
            bloomFilter = bloomFilterPolicy.createFilter();
            // 向布隆过滤器添加所有键
            for (byte[] key : pendingEntries.keySet()) {
                System.out.println("Adding key to bloom filter: " + new String(key));
                bloomFilter.add(key);
            }
            bloomFilterData = bloomFilter.toByteArray();
            System.out.println("Bloom filter size: " + bloomFilterData.length + " bytes");
        }
        
        // 计算需要的缓冲区大小
        int bufferSize = 8; // 条目数和布隆过滤器大小各4字节
        bufferSize += bloomFilterData.length;
        
        // 为每条记录分配大小
        for (Map.Entry<byte[], byte[]> entry : pendingEntries.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();
            
            if (bufferSize == 8 + bloomFilterData.length) {
                // 第一个键，完整存储
                bufferSize += 4 + key.length; // 键长度 + 键
            } else {
                // 后续键，使用前缀压缩
                byte[] prevKey = null;
                for (byte[] k : pendingEntries.keySet()) {
                    if (Arrays.equals(k, key)) {
                        break;
                    }
                    prevKey = k;
                }
                
                int prefixLen = calculatePrefixLength(prevKey, key);
                bufferSize += 8 + (key.length - prefixLen); // 前缀长度 + 后缀长度 + 后缀
            }
            
            bufferSize += 4 + value.length; // 值长度 + 值
        }
        
        // 创建数据块缓冲区 - 使用计算出的实际大小而不是固定大小
        int dataBufferSize = bufferSize;
        
        // 检查是否需要更大的缓冲区
        ByteBuffer tempBuffer = null;
        if (dataBufferSize > dataBlockBuffer.capacity()) {
            System.out.println("Creating larger buffer: " + dataBufferSize + " bytes");
            ByteBuffer largerBuffer = ByteBuffer.allocateDirect(dataBufferSize);
            dataBlockBuffer.clear();
            largerBuffer.put(dataBlockBuffer);
            largerBuffer.clear();
            // 使用临时缓冲区写入数据
            tempBuffer = largerBuffer;
        } else {
            dataBlockBuffer.clear();
            // 使用已有缓冲区
            tempBuffer = dataBlockBuffer;
        }
        
        // 写入条目数
        tempBuffer.putInt(pendingEntries.size());
        
        // 写入布隆过滤器
        tempBuffer.putInt(bloomFilterData.length);
        tempBuffer.put(bloomFilterData);
        
        // 写入键值对
        byte[] firstKey = null;
        for (Map.Entry<byte[], byte[]> entry : pendingEntries.entrySet()) {
            byte[] key = entry.getKey();
            byte[] value = entry.getValue();
            
            if (firstKey == null) {
                firstKey = key;
                
                // 写入完整的第一个键
                tempBuffer.putInt(key.length);
                tempBuffer.put(key);
            } else {
                // 前缀压缩: 计算与前一个键的共同前缀
                int prefixLen = calculatePrefixLength(firstKey, key);
                
                // 写入共享前缀长度、剩余部分长度、剩余部分
                tempBuffer.putInt(prefixLen);
                tempBuffer.putInt(key.length - prefixLen);
                tempBuffer.put(key, prefixLen, key.length - prefixLen);
            }
            
            // 写入值长度和值
            tempBuffer.putInt(value.length);
            tempBuffer.put(value);
        }
        
        // 准备写入
        tempBuffer.flip();
        
        // 记录块的位置和大小
        long blockOffset = currentOffset;
        int blockSize = tempBuffer.remaining();
        
        // 写入数据块
        while (tempBuffer.hasRemaining()) {
            channel.write(tempBuffer);
        }
        
        // 更新偏移量
        currentOffset += blockSize;
        
        // 创建索引条目: 最小键 -> 块位置
        BlockHandle blockHandle = new BlockHandle(blockOffset, blockSize);
        indexBlocks.add(blockHandle);
        
        // 清空待处理条目
        pendingEntries.clear();
        
        return blockHandle;
    }

    /**
     * 计算两个字节数组的共同前缀长度
     * @param a 第一个数组
     * @param b 第二个数组
     * @return 共同前缀长度
     */
    private int calculatePrefixLength(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        int i = 0;
        while (i < minLen && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    /**
     * 写入索引块
     * @return 索引块句柄
     * @throws IOException 如果写入失败
     */
    private BlockHandle writeIndexBlock() throws IOException {
        if (indexBlocks.isEmpty()) {
            return new BlockHandle(0, 0);  // 空表
        }
        
        // 创建索引块缓冲区
        int estimatedSize = indexBlocks.size() * 32;  // 估计每个索引条目32字节
        estimatedSize += indexBlocks.size() * 64;    // 为每个块的键增加空间
        ByteBuffer indexBuffer = ByteBuffer.allocate(estimatedSize);
        
        // 写入索引块数量
        indexBuffer.putInt(indexBlocks.size());
        
        // 写入每个索引条目
        for (int i = 0; i < indexBlocks.size(); i++) {
            BlockHandle handle = indexBlocks.get(i);
            byte[] handleBytes = handle.serialize();
            
            // 写入块句柄
            indexBuffer.putInt(handleBytes.length);
            indexBuffer.put(handleBytes);
            
            // 写入块的最小键
            if (i < blockMinKeysForIndex.size() && blockMinKeysForIndex.get(i) != null) {
                byte[] minKey = blockMinKeysForIndex.get(i);
                indexBuffer.putInt(minKey.length);
                indexBuffer.put(minKey);
            } else {
                // 如果没有最小键信息，写入空键
                indexBuffer.putInt(0);
            }
        }
        
        // 准备写入
        indexBuffer.flip();
        
        // 记录索引块的位置和大小
        long indexOffset = currentOffset;
        int indexSize = indexBuffer.remaining();
        
        // 写入索引块
        channel.write(indexBuffer);
        
        // 更新偏移量
        currentOffset += indexSize;
        
        return new BlockHandle(indexOffset, indexSize);
    }

    /**
     * 写入脚注
     * @param indexBlockHandle 索引块句柄
     * @throws IOException 如果写入失败
     */
    private void writeFooter(BlockHandle indexBlockHandle) throws IOException {
        // 创建元数据
        SSTableMetadata metadata = new SSTableMetadata.Builder()
            .id(tableId)
            .recordCount(recordCount)
            .smallestKey(smallestKey)
            .largestKey(largestKey)
            .creationTime(System.currentTimeMillis())
            .level(0)  // 默认为0级
            .dataSize(indexBlockHandle.getOffset())
            .indexSize(indexBlockHandle.getSize())
            .bloomFilterSize(0)  // 内嵌在数据块中
            .build();
        
        // 序列化元数据
        byte[] metadataBytes = metadata.serialize();
        byte[] handleBytes = indexBlockHandle.serialize();
        
        // 计算所需总大小，确保不超过FOOTER_SIZE
        int requiredSize = 4 + handleBytes.length + 4 + metadataBytes.length + 8; // 句柄长度+句柄+元数据长度+元数据+魔数
        if (requiredSize > FOOTER_SIZE) {
            throw new IOException("Footer size exceeded: " + requiredSize + " > " + FOOTER_SIZE);
        }
        
        ByteBuffer footerBuffer = ByteBuffer.allocate(FOOTER_SIZE);
        
        // 写入索引块句柄
        footerBuffer.putInt(16); // 固定写入 16 作为句柄长度
        footerBuffer.put(handleBytes);
        
        // 写入元数据长度和数据
        footerBuffer.putInt(metadataBytes.length);
        footerBuffer.put(metadataBytes);
        
        // 填充至固定大小
        int remainingBytes = FOOTER_SIZE - footerBuffer.position() - 8; // 减去魔数的8字节
        for (int i = 0; i < remainingBytes; i++) {
            footerBuffer.put((byte) 0);
        }
        
        // 写入魔数
        footerBuffer.putLong(0x73737461626c6500L);  // "sstable\0" 的ASCII码
        
        // 准备写入
        footerBuffer.flip();
        
        // 写入脚注
        channel.write(footerBuffer);
        
        // 更新偏移量
        currentOffset += FOOTER_SIZE;
    }

    /**
     * 完成SSTable构建
     * @return 构建的SSTable路径
     * @throws IOException 如果写入失败
     */
    public Path finish() throws IOException {
        if (!channel.isOpen()) {
            return path; // 已经关闭了
        }
        
        // 写入最后的数据块
        if (!pendingEntries.isEmpty()) {
            flushDataBlock();
        }
        
        // 写入索引块
        BlockHandle indexBlockHandle = writeIndexBlock();
        
        // 写入脚注
        writeFooter(indexBlockHandle);
        
        // 确保数据写入磁盘但不关闭文件
        channel.force(true);
        
        return path;
    }

    @Override
    public void close() throws IOException {
        try {
            if (channel.isOpen()) {
                if (!pendingEntries.isEmpty()) {
                    // 如果还有未写入的数据，完成写入
                    finish();
                }
                // 确保关闭通道
                channel.close();
            }
        } catch (Exception e) {
            // 忽略异常但记录日志
            System.err.println("Error finishing SSTable: " + e.getMessage());
        }
    }
} 