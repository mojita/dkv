package com.mojita.core.lsm.sstable.bloom;

/**
 * 布隆过滤器策略接口
 */
public interface BloomFilterPolicy {
    /**
     * 创建一个新的布隆过滤器
     * @return 布隆过滤器实例
     */
    BloomFilter createFilter();
    
    /**
     * 从字节数组恢复布隆过滤器
     * @param data 序列化的布隆过滤器数据
     * @return 布隆过滤器实例
     */
    BloomFilter restoreFilter(byte[] data);
    
    /**
     * 默认的布隆过滤器策略实现
     */
    class DefaultPolicy implements BloomFilterPolicy {
        private final int bitsPerKey;
        
        /**
         * 构造函数
         * @param bitsPerKey 每个键使用的位数，影响误判率
         */
        public DefaultPolicy(int bitsPerKey) {
            // 至少使用1位
            this.bitsPerKey = Math.max(1, bitsPerKey);
        }
        
        @Override
        public BloomFilter createFilter() {
            // 估计有1000个键的默认大小
            int bitSize = 1000 * bitsPerKey;
            
            // 计算最佳哈希函数数量: (m/n) * ln(2)
            int numHashFunctions = Math.max(1, (int) Math.round(bitsPerKey * 0.693));
            
            return new BloomFilter(bitSize, numHashFunctions);
        }
        
        @Override
        public BloomFilter restoreFilter(byte[] data) {
            return BloomFilter.fromByteArray(data);
        }
    }
} 