package com.mojita.core.lsm.sstable.bloom;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * 布隆过滤器 - 用于快速检查键是否可能存在
 */
public class BloomFilter {
    private final BitSet bits;
    private final int numHashFunctions;
    private final int bitSize;
    
    /**
     * 构造函数
     * @param bitSize 比特位数
     * @param numHashFunctions 哈希函数数量
     */
    public BloomFilter(int bitSize, int numHashFunctions) {
        this.bits = new BitSet(bitSize);
        this.numHashFunctions = numHashFunctions;
        this.bitSize = bitSize;
    }
    
    /**
     * 从字节数组创建布隆过滤器
     * @param data 序列化的布隆过滤器数据
     * @return 布隆过滤器实例
     */
    public static BloomFilter fromByteArray(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int bitSize = buffer.getInt();
        int numHashFunctions = buffer.getInt();
        
        BloomFilter filter = new BloomFilter(bitSize, numHashFunctions);
        
        // 读取位集合
        int byteArraySize = (int) Math.ceil(bitSize / 8.0);
        byte[] bitArray = new byte[byteArraySize];
        buffer.get(bitArray);
        
        // 设置位
        for (int i = 0; i < byteArraySize * 8; i++) {
            if ((bitArray[i / 8] & (1 << (i % 8))) != 0) {
                filter.bits.set(i);
            }
        }
        
        return filter;
    }
    
    /**
     * 添加键到过滤器
     * @param key 要添加的键
     */
    public void add(byte[] key) {
        long hash1 = murmurHash3(key, 0);
        long hash2 = murmurHash3(key, hash1);
        
        for (int i = 0; i < numHashFunctions; i++) {
            long combinedHash = hash1 + i * hash2;
            // 确保combinedHash为正数
            combinedHash = combinedHash < 0 ? ~combinedHash : combinedHash;
            int index = (int) (combinedHash % bitSize);
            bits.set(index);
        }
    }
    
    /**
     * 检查键是否可能存在
     * @param key 要检查的键
     * @return 如果可能存在返回true，如果一定不存在返回false
     */
    public boolean mightContain(byte[] key) {
        long hash1 = murmurHash3(key, 0);
        long hash2 = murmurHash3(key, hash1);
        
        for (int i = 0; i < numHashFunctions; i++) {
            long combinedHash = hash1 + i * hash2;
            // 确保combinedHash为正数
            combinedHash = combinedHash < 0 ? ~combinedHash : combinedHash; 
            int index = (int) (combinedHash % bitSize);
            if (!bits.get(index)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * 转换为字节数组
     * @return 序列化的布隆过滤器
     */
    public byte[] toByteArray() {
        int byteArraySize = (int) Math.ceil(bitSize / 8.0);
        ByteBuffer buffer = ByteBuffer.allocate(8 + byteArraySize);
        
        // 写入元数据
        buffer.putInt(bitSize);
        buffer.putInt(numHashFunctions);
        
        // 写入位集合
        byte[] bitArray = new byte[byteArraySize];
        for (int i = 0; i < bitSize; i++) {
            if (bits.get(i)) {
                bitArray[i / 8] |= (1 << (i % 8));
            }
        }
        buffer.put(bitArray);
        
        return buffer.array();
    }
    
    /**
     * MurmurHash3算法，用于生成哈希值
     * @param key 键
     * @param seed 种子
     * @return 哈希值
     */
    private static long murmurHash3(byte[] key, long seed) {
        long h1 = seed;
        final long c1 = 0x87c37b91114253d5L;
        final long c2 = 0x4cf5ad432745937fL;
        
        for (int i = 0; i < key.length / 8; i++) {
            long k1 = getLongLittleEndian(key, i * 8);
            k1 *= c1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= c2;
            
            h1 ^= k1;
            h1 = Long.rotateLeft(h1, 27);
            h1 = h1 * 5 + 0x52dce729;
        }
        
        // 处理尾部字节
        long k1 = 0;
        for (int i = (key.length / 8) * 8; i < key.length; i++) {
            k1 ^= ((long) key[i] & 0xff) << (8 * (i % 8));
        }
        
        k1 *= c1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= c2;
        h1 ^= k1;
        
        // 混合
        h1 ^= key.length;
        h1 ^= h1 >>> 33;
        h1 *= 0xff51afd7ed558ccdL;
        h1 ^= h1 >>> 33;
        h1 *= 0xc4ceb9fe1a85ec53L;
        h1 ^= h1 >>> 33;
        
        return h1;
    }
    
    /**
     * 从字节数组获取小端序的long值
     * @param bytes 字节数组
     * @param offset 偏移量
     * @return long值
     */
    private static long getLongLittleEndian(byte[] bytes, int offset) {
        return ((long) bytes[offset] & 0xff) |
               (((long) bytes[offset + 1] & 0xff) << 8) |
               (((long) bytes[offset + 2] & 0xff) << 16) |
               (((long) bytes[offset + 3] & 0xff) << 24) |
               (((long) bytes[offset + 4] & 0xff) << 32) |
               (((long) bytes[offset + 5] & 0xff) << 40) |
               (((long) bytes[offset + 6] & 0xff) << 48) |
               (((long) bytes[offset + 7] & 0xff) << 56);
    }
} 