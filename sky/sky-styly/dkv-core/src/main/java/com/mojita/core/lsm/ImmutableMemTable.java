package com.mojita.core.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 不可变内存表 - 用于异步刷盘
 */
public class ImmutableMemTable {
    private final ConcurrentSkipListMap<byte[], byte[]> data;
    private final long size;
    
    /**
     * 构造函数
     * @param data 内存数据
     * @param size 数据大小
     */
    public ImmutableMemTable(ConcurrentSkipListMap<byte[], byte[]> data, long size) {
        this.data = data;
        this.size = size;
    }
    
    /**
     * 获取大小
     * @return 内存占用（字节）
     */
    public long size() {
        return size;
    }
    
    /**
     * 刷盘为SSTable
     * @param path SSTable路径
     * @return 写入的字节数
     */
    public long flushToSSTable(Path path) throws IOException {
        // 这里仅展示基本的刷盘逻辑，实际实现需要更复杂的SSTable格式
        try (FileChannel channel = FileChannel.open(path, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.WRITE)) {
            
            // 计算总大小
            long totalSize = 0;
            for (Entry<byte[], byte[]> entry : data.entrySet()) {
                totalSize += 8;  // key长度和value长度各4字节
                totalSize += entry.getKey().length;
                totalSize += entry.getValue().length;
            }
            
            // 创建缓冲区
            ByteBuffer buffer = ByteBuffer.allocateDirect((int)totalSize);
            
            // 写入数据
            for (Entry<byte[], byte[]> entry : data.entrySet()) {
                byte[] key = entry.getKey();
                byte[] value = entry.getValue();
                
                // 写入key长度
                buffer.putInt(key.length);
                // 写入key
                buffer.put(key);
                // 写入value长度
                buffer.putInt(value.length);
                // 写入value
                buffer.put(value);
            }
            
            // 写入磁盘
            buffer.flip();
            return channel.write(buffer);
        }
    }
    
    /**
     * 获取数据迭代器
     * @return 内部数据的迭代器
     */
    public Iterable<Map.Entry<byte[], byte[]>> entries() {
        return data.entrySet();
    }
    
    /**
     * 查询特定键
     * @param key 键
     * @return 值，不存在则返回null
     */
    public byte[] get(byte[] key) {
        return data.get(key);
    }
} 