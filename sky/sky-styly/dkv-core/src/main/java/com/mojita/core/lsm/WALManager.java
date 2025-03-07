package com.mojita.core.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WAL管理器 - 负责预写日志的管理
 * 简化版实现，实际应用中需要更复杂的设计
 */
public class WALManager {
    // WAL文件路径
    private final Path walPath;
    
    // 文件通道
    private final FileChannel channel;
    
    // 当前位置
    private final AtomicLong position;
    
    // 上次检查点位置
    private final AtomicLong lastCheckpoint;
    
    /**
     * 构造函数
     * @param walPath WAL文件路径
     * @throws IOException 如果打开文件失败
     */
    public WALManager(Path walPath) throws IOException {
        this.walPath = walPath;
        this.channel = FileChannel.open(walPath, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        this.position = new AtomicLong(channel.size());
        this.lastCheckpoint = new AtomicLong(0);
    }
    
    /**
     * 追加条目
     * @param key 键
     * @param value 值
     * @throws IOException 如果写入失败
     */
    public void appendEntry(byte[] key, byte[] value) {
        try {
            // WAL格式: 类型(1) + 时间戳(8) + key长度(4) + key + value长度(4) + value
            int entrySize = 1 + 8 + 4 + key.length + 4 + value.length;
            ByteBuffer buffer = ByteBuffer.allocate(entrySize);
            
            buffer.put((byte) 1); // 1表示PUT操作
            buffer.putLong(System.currentTimeMillis());
            buffer.putInt(key.length);
            buffer.put(key);
            buffer.putInt(value.length);
            buffer.put(value);
            
            buffer.flip();
            
            // 原子写入
            synchronized (this) {
                channel.position(position.get());
                channel.write(buffer);
                channel.force(false);
                position.addAndGet(entrySize);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to append WAL entry", e);
        }
    }
    
    /**
     * 追加删除操作
     * @param key 要删除的键
     * @throws IOException 如果写入失败
     */
    public void appendDeletion(byte[] key) {
        try {
            // WAL格式: 类型(1) + 时间戳(8) + key长度(4) + key
            int entrySize = 1 + 8 + 4 + key.length;
            ByteBuffer buffer = ByteBuffer.allocate(entrySize);
            
            buffer.put((byte) 2); // 2表示DELETE操作
            buffer.putLong(System.currentTimeMillis());
            buffer.putInt(key.length);
            buffer.put(key);
            
            buffer.flip();
            
            // 原子写入
            synchronized (this) {
                channel.position(position.get());
                channel.write(buffer);
                channel.force(false);
                position.addAndGet(entrySize);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to append WAL deletion", e);
        }
    }
    
    /**
     * 更新检查点
     */
    public void updateCheckpoint() {
        lastCheckpoint.set(position.get());
    }
    
    /**
     * 获取最后检查点位置
     * @return 最后检查点位置
     */
    public long getLastCheckpoint() {
        return lastCheckpoint.get();
    }
    
    /**
     * 关闭WAL管理器
     * @throws IOException 如果关闭失败
     */
    public void close() throws IOException {
        channel.close();
    }
} 