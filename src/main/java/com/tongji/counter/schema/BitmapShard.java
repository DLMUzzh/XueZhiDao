package com.tongji.counter.schema;

/**
 * 位图bitmap分片配置与帮助函数。
 * 采用固定分片大小，避免单键因用户ID偏移过大而膨胀。通过分片机制避免单个 Redis 键因用户 ID 偏移过大而导致内存膨胀问题
     4. 设计思想
     分片机制：将大量用户 ID 映射到位图的不同分片中，避免单个 Redis 键存储过多数据。
     内存优化：每个分片固定为 4KB，控制了单键的内存占用上限。
     高效定位：通过简单的数学运算（除法和取模）即可快速确定用户数据所在的分片和位偏移。
     5. 应用场景
     在点赞、收藏等计数场景中，通常使用位图记录用户行为（如某用户是否点赞）。
     由于用户数量可能非常庞大，直接将所有用户 ID 存储在一个位图中会导致键值过大。
     通过 BitmapShard 的分片机制，可以将用户数据分散到多个小分片中，提升性能和可维护性。
     6. 示例说明
     假设有一个用户 ID 为 100000：
     分片编号：chunkOf(100000) = 100000 / 32768 = 3
     位偏移：bitOf(100000) = 100000 % 32768 = 17696
     结论：用户 ID 为 100000 的数据位于第 3 个分片的第 17696 位。
 */
public final class BitmapShard {
    // 每个分片的位数（32K 位 => 4KB/分片）
    public static final int CHUNK_SIZE = 32_768;

    /*
    功能：根据用户 ID 计算其所属的 分片编号（chunk）。
    实现：通过整数除法 userId / CHUNK_SIZE 得到分片编号。
    示例：
    如果 userId = 50000，则 chunkOf(50000) = 50000 / 32768 = 1。
    表示用户 ID 为 50000 的数据属于第 1 个分片。
    **/
    public static long chunkOf(long userId) {
        return userId / CHUNK_SIZE;
    }

    /**
     * 功能：根据用户 ID 计算其在分片内的 位偏移。
     * 实现：通过整数取余 userId % CHUNK_SIZE 得到分片位。
     * 示例：
     * 如果 userId = 50000，则 bitOf(50000) = 50000 % 32768 = 17232。
     * 表示用户 ID 为 50000 的数据在第 17232 位。
     **/
    public static long bitOf(long userId) {
        return userId % CHUNK_SIZE;
    }

    private BitmapShard() {}
}
