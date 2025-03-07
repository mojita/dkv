package com.mojita.core.lsm;

import com.mojita.core.lsm.config.MemTableConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultMemTableTest {
    @TempDir
    Path tempFolder;

    private WALManager walManager;
    private MemTable memTable;

    @BeforeEach
    public void setup() throws Exception {
        Path walPath = Files.createFile(tempFolder.resolve("test.wal"));
        walManager = new WALManager(walPath);

        // 创建一个小容量的MemTable用于测试
        MemTableConfig config = new MemTableConfig(1000, 3600000);
        memTable = new DefaultMemTable(config, walManager);

        // 验证初始化是否成功
        assertNotNull(walManager, "WAL Manager should not be null");
        assertNotNull(memTable, "MemTable should not be null");
    }

    @Test
    public void testBasicOperations() {
        // 确保memTable已正确初始化
        assertNotNull(memTable, "MemTable should be initialized");

        byte[] key1 = "key1".getBytes();
        byte[] value1 = "value1".getBytes();

        // 测试写入
        assertFalse(memTable.put(key1, value1));

        // 测试读取
        assertArrayEquals(value1, memTable.get(key1));

        // 测试不存在的键
        assertNull(memTable.get("nonexistent".getBytes()));

        // 测试更新
        byte[] newValue = "newvalue".getBytes();
        assertFalse(memTable.put(key1, newValue));
        assertArrayEquals(newValue, memTable.get(key1));

        // 测试删除
        assertFalse(memTable.delete(key1));
        assertNull(memTable.get(key1));
    }

    @Test
    public void testSizeTracking() {
        // 确保walManager已正确初始化
        assertNotNull(walManager, "WAL Manager should be initialized");

        // 创建新的MemTable实例，避免与其他测试共享状态
        MemTableConfig config = new MemTableConfig(100, 3600000);
        MemTable testMemTable = new DefaultMemTable(config, walManager);

        boolean flushTriggered = false;
        // 写入足够大的数据来触发刷盘
        for (int i = 0; i < 10; i++) {
            byte[] key = ("key" + i).getBytes();
            byte[] value = ("value" + i + "1234567890").getBytes(); // 增加value长度
            boolean shouldFlush = testMemTable.put(key, value);


            System.out.println("Write entry " + i + ", size: " + testMemTable.size());

            if (shouldFlush) {
                // 验证实际大小是否达到阈值
                assertTrue(testMemTable.size() >= config.getMaxSize(), 
                        "MemTable size should meet flush threshold");
                flushTriggered = true;
                break;
            }
        }
        // 确保最终触发了刷盘
        assertTrue(flushTriggered, "Flush should be triggered before loop end");
    }
    @Test
    public void testSwitchToImmutable() {
        // 写入一些数据
        for (int i = 0; i < 5; i++) {
            byte[] key = ("key" + i).getBytes();
            byte[] value = ("value" + i).getBytes();
            memTable.put(key, value);
        }

        // 转换为不可变MemTable
        ImmutableMemTable immutable = memTable.switchToImmutable();

        // 验证数据已转移
        assertNotNull(immutable);
        assertEquals(memTable.size(), immutable.size());

        // 验证可以从不可变表读取
        for (int i = 0; i < 5; i++) {
            byte[] key = ("key" + i).getBytes();
            byte[] value = ("value" + i).getBytes();
            assertArrayEquals(value, immutable.get(key));
        }
    }
}