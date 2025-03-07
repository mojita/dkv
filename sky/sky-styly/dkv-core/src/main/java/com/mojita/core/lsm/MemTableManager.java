package com.mojita.core.lsm;

import com.mojita.core.lsm.config.MemTableConfig;

import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MemTable管理器 - 负责MemTable的生命周期管理和刷盘
 */
public class MemTableManager {
    // 活跃的MemTable
    private final AtomicReference<MemTable> activeMemTable;
    
    // 等待刷盘的不可变MemTable队列
    private final Queue<ImmutableMemTable> immutableMemTables;
    
    // 刷盘线程池
    private final ExecutorService flushExecutor;
    
    // WAL管理器
    private final WALManager walManager;
    
    // 配置
    private final MemTableConfig config;
    
    // SSTable目录
    private final Path sstableDir;
    
    /**
     * 构造函数
     * @param config MemTable配置
     * @param walManager WAL管理器
     * @param sstableDir SSTable目录
     */
    public MemTableManager(MemTableConfig config, WALManager walManager, Path sstableDir) {
        this.config = config;
        this.walManager = walManager;
        this.sstableDir = sstableDir;
        
        this.activeMemTable = new AtomicReference<>(new DefaultMemTable(config, walManager));
        this.immutableMemTables = new ConcurrentLinkedQueue<>();
        this.flushExecutor = Executors.newSingleThreadExecutor();
        
        // 启动后台刷盘任务
        startFlushTask();
    }
    
    /**
     * 写入键值对
     * @param key 键
     * @param value 值
     */
    public void put(byte[] key, byte[] value) {
        while (true) {
            MemTable current = activeMemTable.get();
            boolean needFlush = current.put(key, value);
            
            if (needFlush && activeMemTable.compareAndSet(current, new DefaultMemTable(config, walManager))) {
                // 转换为不可变MemTable并加入队列
                immutableMemTables.offer(current.switchToImmutable());
                // 触发刷盘
                triggerFlush();
            } else if (!needFlush) {
                // 不需要刷盘，直接返回
                break;
            }
            // 需要刷盘但CAS失败，重试
        }
    }
    
    /**
     * 获取值
     * @param key 键
     * @return 值，不存在则返回null
     */
    public byte[] get(byte[] key) {
        // 首先检查活跃的MemTable
        byte[] value = activeMemTable.get().get(key);
        if (value != null) {
            return value;
        }
        
        // 然后检查所有不可变MemTable，从新到旧
        for (ImmutableMemTable immutable : immutableMemTables) {
            value = immutable.get(key);
            if (value != null) {
                return value;
            }
        }
        
        // 内存中未找到，需要查询磁盘（SSTable）
        return null;
    }
    
    /**
     * 删除键
     * @param key 键
     */
    public void delete(byte[] key) {
        while (true) {
            MemTable current = activeMemTable.get();
            boolean needFlush = current.delete(key);
            
            if (needFlush && activeMemTable.compareAndSet(current, new DefaultMemTable(config, walManager))) {
                // 转换为不可变MemTable并加入队列
                immutableMemTables.offer(current.switchToImmutable());
                // 触发刷盘
                triggerFlush();
            } else if (!needFlush) {
                // 不需要刷盘，直接返回
                break;
            }
            // 需要刷盘但CAS失败，重试
        }
    }
    
    /**
     * 启动后台刷盘任务
     */
    private void startFlushTask() {
        flushExecutor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // 检查是否有需要刷盘的MemTable
                    if (!immutableMemTables.isEmpty()) {
                        flushNextMemTable();
                    }
                    
                    // 休眠一段时间
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // 记录异常但继续运行
                    e.printStackTrace();
                }
            }
        });
    }
    
    /**
     * 触发刷盘
     */
    private void triggerFlush() {
        // 唤醒刷盘线程
        flushExecutor.submit(this::flushNextMemTable);
    }
    
    /**
     * 刷盘下一个MemTable
     */
    private void flushNextMemTable() {
        ImmutableMemTable memTable = immutableMemTables.peek();
        if (memTable == null) {
            return;
        }
        
        try {
            // 生成SSTable文件名
            String fileName = "sst_" + System.currentTimeMillis() + ".sst";
            Path sstablePath = sstableDir.resolve(fileName);
            
            // 刷盘
            memTable.flushToSSTable(sstablePath);
            
            // 移除已刷盘的MemTable
            immutableMemTables.poll();
            
            // 更新WAL检查点
            walManager.updateCheckpoint();
        } catch (Exception e) {
            // 记录异常但不移除MemTable，下次重试
            e.printStackTrace();
        }
    }
    
    /**
     * 关闭管理器
     */
    public void close() {
        flushExecutor.shutdown();
        // 确保所有不可变MemTable都刷盘
        while (!immutableMemTables.isEmpty()) {
            flushNextMemTable();
        }
        // 确保活跃MemTable也刷盘
        ImmutableMemTable lastMemTable = activeMemTable.get().switchToImmutable();
        try {
            String fileName = "sst_" + System.currentTimeMillis() + ".sst";
            Path sstablePath = sstableDir.resolve(fileName);
            lastMemTable.flushToSSTable(sstablePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
} 