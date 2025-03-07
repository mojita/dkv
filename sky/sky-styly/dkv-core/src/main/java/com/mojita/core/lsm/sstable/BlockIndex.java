package com.mojita.core.lsm.sstable;

import java.util.Arrays;
import java.util.Comparator;

/**
 * 块索引 - 快速定位键所在的数据块
 */
public class BlockIndex {
    private final BlockHandle[] blockHandles;
    private final byte[][] blockKeys; // 每个块的最小键
    
    /**
     * 构造函数
     * @param blockHandles 块句柄数组
     */
    public BlockIndex(BlockHandle[] blockHandles) {
        this.blockHandles = blockHandles;
        this.blockKeys = new byte[blockHandles.length][];
    }
    
    /**
     * 设置块的最小键
     * @param blockIndex 块索引
     * @param key 最小键
     */
    public void setBlockKey(int blockIndex, byte[] key) {
        if (blockIndex >= 0 && blockIndex < blockKeys.length) {
            blockKeys[blockIndex] = key;
        }
    }
    
    /**
     * 查找包含特定键的块句柄
     * @param key 要查找的键
     * @return 块句柄，如果未找到返回null
     */
    public BlockHandle findBlockHandle(byte[] key) {
        // 检查索引是否为空
        if (blockHandles.length == 0) {
            System.out.println("Empty index");
            return null;
        }
        
        // 如果只有一个数据块且没有键信息，直接返回第一个块
        if (blockHandles.length == 1 && (blockKeys == null || blockKeys.length == 0 || blockKeys[0] == null)) {
            System.out.println("Single block with no key - returning it directly");
            return blockHandles[0];
        }
        
        // 创建字节数组比较器
        Comparator<byte[]> comparator = (a, b) -> {
            int minLen = Math.min(a.length, b.length);
            for (int i = 0; i < minLen; i++) {
                int cmp = Byte.compare(a[i], b[i]);
                if (cmp != 0) return cmp;
            }
            return Integer.compare(a.length, b.length);
        };
        
        // 对于小数据集，直接扫描可能更简单
        if (blockHandles.length < 10) {
            for (int i = 0; i < blockHandles.length; i++) {
                if (blockKeys[i] != null) {
                    // 使用安全的方法显示二进制键
                    System.out.println("Block " + i + " with min key: " + bytesToHex(blockKeys[i]) + 
                                      ", searching for: " + bytesToHex(key));
                    
                    if (i == blockHandles.length - 1 || 
                        comparator.compare(key, blockKeys[i + 1]) < 0) {
                        if (comparator.compare(key, blockKeys[i]) >= 0) {
                            return blockHandles[i];
                        }
                    }
                }
            }
            return null;
        }
        
        // 二分查找
        System.out.println("Using binary search for " + blockHandles.length + " blocks");
        int low = 0;
        int high = blockKeys.length - 1;
        
        while (low <= high) {
            int mid = (low + high) >>> 1;
            System.out.println("Binary search: low=" + low + ", high=" + high + ", mid=" + mid);
            
            if (mid == blockKeys.length - 1 || comparator.compare(key, blockKeys[mid + 1]) < 0) {
                // 键小于下一个块的最小键
                if (comparator.compare(key, blockKeys[mid]) >= 0) {
                    // 键大于等于当前块的最小键
                    System.out.println("Found block at index: " + mid);
                    return blockHandles[mid];
                } else {
                    high = mid - 1;
                }
            } else {
                low = mid + 1;
            }
        }
        
        System.out.println("No block found for key");
        return null;
    }
    
    /**
     * 获取所有块句柄
     * @return 块句柄数组
     */
    public BlockHandle[] getBlockHandles() {
        return Arrays.copyOf(blockHandles, blockHandles.length);
    }
    
    /**
     * 获取块数量
     * @return 块数量
     */
    public int size() {
        return blockHandles.length;
    }

    /**
     * 将字节数组转换为十六进制字符串，用于安全显示
     * @param bytes 字节数组
     * @return 十六进制字符串
     */
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        
        // 尝试将其作为UTF-8字符串，如果失败则使用十六进制
        try {
            String str = new String(bytes, "UTF-8");
            // 检查是否只包含可打印ASCII字符
            boolean isPrintable = true;
            for (int i = 0; i < str.length(); i++) {
                if (str.charAt(i) < 32 || str.charAt(i) > 126) {
                    isPrintable = false;
                    break;
                }
            }
            if (isPrintable) {
                return "\"" + str + "\"";
            }
        } catch (Exception e) {
            // 忽略异常，使用十六进制
        }
        
        // 转为十六进制
        StringBuilder result = new StringBuilder();
        result.append("0x");
        for (byte b : bytes) {
            result.append(String.format("%02X", b));
        }
        return result.toString();
    }
} 