package com.mojita.core.lsm;

import com.mojita.core.lsm.config.MemTableConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * LSM树引擎 - 提供高级接口
 */
public class LSMEngine implements AutoCloseable {
    private final MemTableManager memTableManager;
    private final WALManager walManager;
    private final Path dataDir;
    
    /**
     * 构造函数
     * @param dataDir 数据目录
     * @throws IOException 如果创建目录失败
     */
    public LSMEngine(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        
        // 创建必要的目录
        Path walDir = dataDir.resolve("wal");
        Path sstableDir = dataDir.resolve("sst");
        Files.createDirectories(walDir);
        Files.createDirectories(sstableDir);
        
        // 创建WAL管理器
        Path walPath = walDir.resolve("current.wal");
        this.walManager = new WALManager(walPath);
        
        // 创建MemTable管理器
        MemTableConfig config = new MemTableConfig();
        this.memTableManager = new MemTableManager(config, walManager, sstableDir);
    }
    
    /**
     * 写入键值对
     * @param key 键
     * @param value 值
     */
    public void put(byte[] key, byte[] value) {
        memTableManager.put(key, value);
    }
    
    /**
     * 获取值
     * @param key 键
     * @return 值，不存在则返回null
     */
    public byte[] get(byte[] key) {
        return memTableManager.get(key);
    }
    
    /**
     * 删除键
     * @param key 键
     */
    public void delete(byte[] key) {
        memTableManager.delete(key);
    }
    
    /**
     * 关闭引擎
     */
    @Override
    public void close() throws Exception {
        memTableManager.close();
        walManager.close();
    }
} 