package com.mojita.core.lsm;

import com.mojita.core.lsm.config.MemTableConfig;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Comparator;

/**
 * 测试专用MemTable实现，不依赖WAL
 */
public class TestMemTable implements MemTable {
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
    
    private final ConcurrentSkipListMap<byte[], byte[]> data;
    private final AtomicLong currentSize;
    private final MemTableConfig config;
    
    public TestMemTable(MemTableConfig config) {
        this.data = new ConcurrentSkipListMap<>(BYTES_COMPARATOR);
        this.currentSize = new AtomicLong(0);
        this.config = config;
    }
    
    @Override
    public boolean put(byte[] key, byte[] value) {
        // 不写WAL，只更新内存
        byte[] oldValue = data.put(key, value);
        
        if (oldValue != null) {
            currentSize.addAndGet(key.length + value.length - oldValue.length);
        } else {
            currentSize.addAndGet(key.length + value.length + 16);
        }
        
        return shouldFlush();
    }
    
    @Override
    public byte[] get(byte[] key) {
        return data.get(key);
    }
    
    @Override
    public boolean delete(byte[] key) {
        byte[] oldValue = data.remove(key);
        
        if (oldValue != null) {
            currentSize.addAndGet(-(key.length + oldValue.length));
        }
        
        return shouldFlush();
    }
    
    @Override
    public long size() {
        return currentSize.get();
    }
    
    @Override
    public boolean shouldFlush() {
        return currentSize.get() >= config.getMaxSize();
    }
    
    @Override
    public ImmutableMemTable switchToImmutable() {
        return new ImmutableMemTable(data, currentSize.get());
    }
} 