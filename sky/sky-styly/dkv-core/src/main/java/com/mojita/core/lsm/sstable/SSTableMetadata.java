package com.mojita.core.lsm.sstable;

import java.nio.ByteBuffer;

/**
 * SSTable元数据，存储表的统计信息和属性
 */
public class SSTableMetadata {
    private final long id;                // SSTable ID
    private final long recordCount;       // 记录数量
    private final byte[] smallestKey;     // 最小键
    private final byte[] largestKey;      // 最大键
    private final long creationTime;      // 创建时间
    private final int level;              // 层级（用于分层压缩）
    private final long dataSize;          // 数据区大小
    private final long indexSize;         // 索引区大小
    private final long bloomFilterSize;   // 布隆过滤器大小（保留为0表示使用块级过滤器）
    
    private SSTableMetadata(Builder builder) {
        this.id = builder.id;
        this.recordCount = builder.recordCount;
        this.smallestKey = builder.smallestKey;
        this.largestKey = builder.largestKey;
        this.creationTime = builder.creationTime;
        this.level = builder.level;
        this.dataSize = builder.dataSize;
        this.indexSize = builder.indexSize;
        this.bloomFilterSize = builder.bloomFilterSize;
    }
    
    public long getId() {
        return id;
    }
    
    public long getRecordCount() {
        return recordCount;
    }
    
    public byte[] getSmallestKey() {
        return smallestKey;
    }
    
    public byte[] getLargestKey() {
        return largestKey;
    }
    
    public long getCreationTime() {
        return creationTime;
    }
    
    public int getLevel() {
        return level;
    }
    
    public long getDataSize() {
        return dataSize;
    }
    
    public long getIndexSize() {
        return indexSize;
    }
    
    public long getBloomFilterSize() {
        return bloomFilterSize;
    }
    
    /**
     * 序列化元数据到字节数组
     * @return 序列化后的字节数组
     */
    public byte[] serialize() {
        // 计算总大小
        int smallestKeyLen = smallestKey != null ? smallestKey.length : 0;
        int largestKeyLen = largestKey != null ? largestKey.length : 0;
        // 安全地计算所需缓冲区大小 - 考虑对齐和可能的额外字段
        int totalSize = 8 + 8 + 4 + smallestKeyLen + 4 + largestKeyLen + 8 + 4 + 8 + 8 + 8 + 8 + 16;
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putLong(id);
        buffer.putLong(recordCount);
        
        buffer.putInt(smallestKey != null ? smallestKey.length : 0);
        if (smallestKey != null) {
            buffer.put(smallestKey);
        }
        
        buffer.putInt(largestKey != null ? largestKey.length : 0);
        if (largestKey != null) {
            buffer.put(largestKey);
        }
        
        buffer.putLong(creationTime);
        buffer.putInt(level);
        buffer.putLong(dataSize);
        buffer.putLong(indexSize);
        buffer.putLong(bloomFilterSize);
        buffer.putLong(0);  // 为了兼容性，保留一个Long字段的位置，替代移除的bloomFilterOffset
        
        // 在实际返回前，修剪到实际使用的大小
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
    
    /**
     * 从字节数组反序列化元数据
     * @param bytes 序列化的元数据
     * @return 元数据对象
     */
    public static SSTableMetadata deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        
        Builder builder = new Builder();
        builder.id(buffer.getLong());
        builder.recordCount(buffer.getLong());
        
        int smallestKeyLen = buffer.getInt();
        if (smallestKeyLen > 0) {
            byte[] smallestKey = new byte[smallestKeyLen];
            buffer.get(smallestKey);
            builder.smallestKey(smallestKey);
        }
        
        int largestKeyLen = buffer.getInt();
        if (largestKeyLen > 0) {
            byte[] largestKey = new byte[largestKeyLen];
            buffer.get(largestKey);
            builder.largestKey(largestKey);
        }
        
        builder.creationTime(buffer.getLong());
        builder.level(buffer.getInt());
        builder.dataSize(buffer.getLong());
        builder.indexSize(buffer.getLong());
        
        // 安全读取剩余字段，避免缓冲区溢出
        if (buffer.remaining() >= 8) {
            builder.bloomFilterSize(buffer.getLong());
        } else {
            builder.bloomFilterSize(0);
        }
        
        // 只有在还有足够字节的情况下才读取额外字段
        if (buffer.remaining() >= 8) {
            buffer.getLong();  // 读取但忽略额外的8字节
        }
        
        return builder.build();
    }
    
    /**
     * 元数据构建器
     */
    public static class Builder {
        private long id;
        private long recordCount;
        private byte[] smallestKey;
        private byte[] largestKey;
        private long creationTime = System.currentTimeMillis();
        private int level;
        private long dataSize;
        private long indexSize;
        private long bloomFilterSize;
        
        public Builder id(long id) {
            this.id = id;
            return this;
        }
        
        public Builder recordCount(long recordCount) {
            this.recordCount = recordCount;
            return this;
        }
        
        public Builder smallestKey(byte[] smallestKey) {
            this.smallestKey = smallestKey;
            return this;
        }
        
        public Builder largestKey(byte[] largestKey) {
            this.largestKey = largestKey;
            return this;
        }
        
        public Builder creationTime(long creationTime) {
            this.creationTime = creationTime;
            return this;
        }
        
        public Builder level(int level) {
            this.level = level;
            return this;
        }
        
        public Builder dataSize(long dataSize) {
            this.dataSize = dataSize;
            return this;
        }
        
        public Builder indexSize(long indexSize) {
            this.indexSize = indexSize;
            return this;
        }
        
        public Builder bloomFilterSize(long size) {
            this.bloomFilterSize = size;
            return this;
        }
        
        public SSTableMetadata build() {
            return new SSTableMetadata(this);
        }
    }
} 