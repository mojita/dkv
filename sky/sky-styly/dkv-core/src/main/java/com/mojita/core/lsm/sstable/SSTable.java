package com.mojita.core.lsm.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

/**
 * SSTable - 排序字符串表，LSM树的磁盘存储组件
 * 不可变的、持久化的键值对集合
 */
public interface SSTable extends Closeable, Iterable<Map.Entry<byte[], byte[]>> {
    /**
     * 获取SSTable的唯一标识
     * @return SSTable ID
     */
    long getId();
    
    /**
     * 获取SSTable文件路径
     * @return 文件路径
     */
    Path getPath();
    
    /**
     * 获取SSTable的估计大小（字节）
     * @return 文件大小
     */
    long size();
    
    /**
     * 查找特定键的值
     * @param key 键
     * @return 如果存在则返回值，否则返回null
     * @throws IOException 如果读取出错
     */
    byte[] get(byte[] key) throws IOException;
    
    /**
     * 获取键值对迭代器
     * @return 键值对迭代器
     * @throws IOException 如果读取出错
     */
    @Override
    Iterator<Map.Entry<byte[], byte[]>> iterator();
    
    /**
     * 获取SSTable的元数据
     * @return 元数据
     */
    SSTableMetadata getMetadata();
    
    /**
     * 判断键是否可能存在（基于布隆过滤器，可能有误判）
     * @param key 键
     * @return 如果可能存在返回true，如果一定不存在返回false
     */
    boolean mayContain(byte[] key);
} 