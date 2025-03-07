package com.mojita.core.lsm;

/**
 * MemTable接口 - LSM-Tree的内存表组件
 * 负责管理内存中的键值对，提供快速读写操作
 */
public interface MemTable {
    /**
     * 写入键值对
     * @param key 键
     * @param value 值
     * @return 是否导致内存表切换
     */
    boolean put(byte[] key, byte[] value);
    
    /**
     * 获取值
     * @param key 键
     * @return 对应的值，不存在则返回null
     */
    byte[] get(byte[] key);
    
    /**
     * 删除键
     * @param key 键
     * @return 是否导致内存表切换
     */
    boolean delete(byte[] key);
    
    /**
     * 获取当前估计大小
     * @return 当前占用的内存大小（字节）
     */
    long size();
    
    /**
     * 检查是否需要刷盘
     * @return 是否达到刷盘阈值
     */
    boolean shouldFlush();
    
    /**
     * 转换为不可变MemTable
     * @return 转换后的不可变MemTable
     */
    ImmutableMemTable switchToImmutable();
} 