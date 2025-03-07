package com.mojita.core.lsm.sstable;

import com.mojita.core.lsm.sstable.bloom.BloomFilterPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class SSTableTest {
    @TempDir
    Path tempDir;
    
    private Path sstablePath;
    private Map<byte[], byte[]> testData;
    
    @BeforeEach
    public void setup() {
        // 为每个测试使用唯一文件名，避免冲突
        Path testDataDir = Paths.get(System.getProperty("user.dir"), "target", "test-data");
        if (!Files.exists(testDataDir)) {
            try {
                Files.createDirectories(testDataDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        sstablePath = testDataDir.resolve("test_" + System.currentTimeMillis() + ".sst");
        testData = new HashMap<>();
        
        // 准备测试数据
        for (int i = 0; i < 100; i++) {
            byte[] key = ("key" + String.format("%03d", i)).getBytes();
            byte[] value = ("value" + i).getBytes();
            testData.put(key, value);
        }
    }
    
    @AfterEach
    public void cleanup() {
        testData.clear();
    }
    
    @Test
    public void testWriteAndReadSSTable() throws IOException {
        // 创建一个新的SSTable并写入数据
        // 不使用try-with-resources，确保显式关闭
        SSTableBuilder builder = new SSTableBuilder(sstablePath, 1);
        for (Map.Entry<byte[], byte[]> entry : testData.entrySet()) {
            builder.add(entry.getKey(), entry.getValue());
        }
        builder.finish();  // 先完成所有写入
        builder.close();   // 显式关闭
        
        // 验证文件存在且有内容
        assertTrue(Files.exists(sstablePath), "SSTable file should exist");
        assertTrue(Files.size(sstablePath) > 0, "SSTable file should have content");
        
        // 读取SSTable并验证数据
        try (SSTableReader reader = new SSTableReader(sstablePath)) {
            assertEquals(1, reader.getId());
            
            // 检查元数据
            SSTableMetadata metadata = reader.getMetadata();
            assertEquals(testData.size(), metadata.getRecordCount());
            
            // 检查所有键值对
            for (Map.Entry<byte[], byte[]> entry : testData.entrySet()) {
                byte[] value = reader.get(entry.getKey());
                assertNotNull(value);
                assertArrayEquals(entry.getValue(), value);
            }
            
            // 检查不存在的键
            assertNull(reader.get("nonexistent".getBytes()));
        }
    }
    
    @Test
    public void testBloomFilterEffectiveness() throws IOException {
        // 创建一个使用布隆过滤器的SSTable
        // 增加位/键值，减少误判率
        BloomFilterPolicy bloomFilterPolicy = new BloomFilterPolicy.DefaultPolicy(16);
        
        // 不使用try-with-resources，确保显式关闭
        SSTableBuilder builder = new SSTableBuilder(sstablePath, 1, 4096, bloomFilterPolicy);
        for (Map.Entry<byte[], byte[]> entry : testData.entrySet()) {
            builder.add(entry.getKey(), entry.getValue());
        }
        builder.finish();  // 先完成所有写入
        builder.close();   // 显式关闭
        
        // 验证文件存在且有内容
        assertTrue(Files.exists(sstablePath), "SSTable file should exist");
        assertTrue(Files.size(sstablePath) > 0, "SSTable file should have content");
        
        // 验证布隆过滤器的有效性
        try (SSTableReader reader = new SSTableReader(sstablePath)) {
            // 所有存在的键都应该通过布隆过滤器
            System.out.println("\nTesting existing keys in bloom filter:");
            for (byte[] key : testData.keySet()) {
                String keyStr = new String(key);
                boolean result = reader.mayContain(key);
                System.out.println("Key " + keyStr + " exists: " + result);
                assertTrue(result, "Key should exist in bloom filter: " + keyStr);
            }
            
            // 测试布隆过滤器的误判率
            System.out.println("\nTesting random keys in bloom filter:");
            // 生成一些随机不存在的键
            Random random = new Random(42);
            int falsePositives = 0;
            int totalTests = 100; // 降低测试数量使调试更快
            
            for (int i = 0; i < totalTests; i++) {
                byte[] randomKey = new byte[10];
                random.nextBytes(randomKey);
                
                // 检查随机键是否被认为可能存在
                if (reader.mayContain(randomKey)) {
                    falsePositives++;
                }
            }
            
            // 布隆过滤器的误判率应该小于一定阈值
            double falsePositiveRate = (double) falsePositives / totalTests;
            System.out.printf("Bloom filter false positive rate: %.2f%%\n", falsePositiveRate * 100);
            assertTrue(falsePositiveRate < 0.5, "False positive rate should be reasonable");
        }
    }
    
    @Test
    public void testSSTableIterator() throws IOException {
        // 创建一个新的SSTable
        // 不使用try-with-resources，确保显式关闭
        SSTableBuilder builder = new SSTableBuilder(sstablePath, 1);
        for (Map.Entry<byte[], byte[]> entry : testData.entrySet()) {
            builder.add(entry.getKey(), entry.getValue());
        }
        builder.finish();  // 先完成所有写入
        builder.close();   // 显式关闭
        
        // 验证文件存在且有内容
        assertTrue(Files.exists(sstablePath), "SSTable file should exist");
        assertTrue(Files.size(sstablePath) > 0, "SSTable file should have content");
        
        // 使用迭代器读取所有内容
        try (SSTableReader reader = new SSTableReader(sstablePath)) {
            Map<String, String> readData = new HashMap<>();
            
            // 使用迭代器遍历所有条目
            for (Map.Entry<byte[], byte[]> entry : reader) {
                String key = new String(entry.getKey());
                String value = new String(entry.getValue());
                readData.put(key, value);
            }
            
            // 验证是否读取了所有内容
            assertEquals(testData.size(), readData.size());
            
            // 验证内容是否正确
            for (Map.Entry<byte[], byte[]> entry : testData.entrySet()) {
                String key = new String(entry.getKey());
                String expectedValue = new String(entry.getValue());
                String actualValue = readData.get(key);
                
                assertEquals(expectedValue, actualValue);
            }
        }
    }
} 