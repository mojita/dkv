package com.mojita.core.lsm;

import com.mojita.core.lsm.config.MemTableConfig;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Arrays;
import java.util.Comparator;

/**
 * MemTable默认实现 - 使用ConcurrentSkipListMap作为内部存储
 */
public class DefaultMemTable implements MemTable {
    // 字节数组比较器
    private static final Comparator<byte[]> BYTES_COMPARATOR = (a, b) -> {
        int minLength = Math.min(a.length, b.length);
        for (int i = 0; i < minLength; i++) {
            int diff = Byte.compare(a[i], b[i]);
            if (diff != 0) {
                return diff;
            }
        }
        return Integer.compare(a.length, b.length);
    };
    
    // 内部数据存储
    private final ConcurrentSkipListMap<byte[], byte[]> data;
    
    // 当前大小追踪
    private final AtomicLong currentSize;
    
    // 配置参数
    private final MemTableConfig config;
    
    // WAL管理器
    private final WALManager walManager;
    
    // 创建时间
    private final long createdTimeMs;
    
    /**
     * 构造函数
     * @param config 内存表配置
     * @param walManager WAL管理器
     */
    public DefaultMemTable(MemTableConfig config, WALManager walManager) {
        this.data = new ConcurrentSkipListMap<>(BYTES_COMPARATOR);
        this.currentSize = new AtomicLong(0);
        this.config = config;
        this.walManager = walManager;
        this.createdTimeMs = System.currentTimeMillis();
    }
    
    @Override
    public boolean put(byte[] key, byte[] value) {
        // 添加空指针检查
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }
        if (walManager == null) {
            throw new IllegalStateException("WAL Manager is not initialized");
        }

        // 写入WAL
        walManager.appendEntry(key, value);
        
        // 更新内存表
        byte[] oldValue = data.put(key, value);
        
        // 更新内存占用
        if (oldValue != null) {
            currentSize.addAndGet(key.length + value.length - oldValue.length);
        } else {
            currentSize.addAndGet(key.length + value.length + 16); // 16字节为SkipList节点开销
        }
        
        // 检查是否需要刷盘
        return shouldFlush();
    }
    
    @Override
    public byte[] get(byte[] key) {
        return data.get(key);
    }
    
    @Override
    public boolean delete(byte[] key) {
        // 写入WAL（删除标记）
        walManager.appendDeletion(key);
        
        // 从内存表删除
        byte[] oldValue = data.remove(key);
        
        // 更新内存占用
        if (oldValue != null) {
            currentSize.addAndGet(-(key.length + oldValue.length));
        }
        
        // 检查是否需要刷盘
        return shouldFlush();
    }
    
    @Override
    public long size() {
        return currentSize.get();
    }
    
    @Override
    public boolean shouldFlush() {
        // 检查大小阈值
        if (currentSize.get() >= config.getMaxSize()) {
            return true;
        }
        
        // 检查时间阈值
        if (config.getMaxLifetimeMs() > 0 && 
            System.currentTimeMillis() - createdTimeMs > config.getMaxLifetimeMs()) {
            return true;
        }
        
        return false;
    }
    
    @Override
    public ImmutableMemTable switchToImmutable() {
        return new ImmutableMemTable(data, currentSize.get());
    }
} 