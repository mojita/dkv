package com.mojita.core.lsm.config;

/**
 * MemTable配置参数
 */
public class MemTableConfig {
    // 默认最大大小 (4MB)
    private static final long DEFAULT_MAX_SIZE = 4 * 1024 * 1024;
    
    // 默认最大生命周期 (30分钟)
    private static final long DEFAULT_MAX_LIFETIME_MS = 30 * 60 * 1000;
    
    private long maxSize;
    private long maxLifetimeMs;
    
    /**
     * 默认构造函数
     */
    public MemTableConfig() {
        this.maxSize = DEFAULT_MAX_SIZE;
        this.maxLifetimeMs = DEFAULT_MAX_LIFETIME_MS;
    }
    
    /**
     * 完整构造函数
     * @param maxSize 最大大小（字节）
     * @param maxLifetimeMs 最大生命周期（毫秒）
     */
    public MemTableConfig(long maxSize, long maxLifetimeMs) {
        this.maxSize = maxSize;
        this.maxLifetimeMs = maxLifetimeMs;
    }
    
    public long getMaxSize() {
        return maxSize;
    }
    
    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }
    
    public long getMaxLifetimeMs() {
        return maxLifetimeMs;
    }
    
    public void setMaxLifetimeMs(long maxLifetimeMs) {
        this.maxLifetimeMs = maxLifetimeMs;
    }
} 