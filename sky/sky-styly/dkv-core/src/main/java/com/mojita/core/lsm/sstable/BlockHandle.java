package com.mojita.core.lsm.sstable;

import java.nio.ByteBuffer;

/**
 * 块句柄 - 表示文件中一个数据块的位置和大小
 */
public class BlockHandle {
    private final long offset;  // 块在文件中的起始位置
    private final long size;    // 块的大小

    public BlockHandle(long offset, long size) {
        this.offset = offset;
        this.size = size;
    }

    public long getOffset() {
        return offset;
    }

    public long getSize() {
        return size;
    }

    /**
     * 序列化块句柄到字节数组
     * @return 序列化后的字节数组
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(offset);
        buffer.putLong(size);
        return buffer.array();
    }

    /**
     * 从字节数组反序列化块句柄
     * @param bytes 序列化的块句柄
     * @return 块句柄对象
     */
    public static BlockHandle deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long offset = buffer.getLong();
        long size = buffer.getLong();
        return new BlockHandle(offset, size);
    }
} 