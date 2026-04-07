package com.tongji.counter.schema;

/**
 * Redis Key 生成工具。
 * 3. 设计思想
     * 规范化命名： 所有键均采用统一的前缀（如 cnt、bm、agg）和层级结构，便于管理和调试。
     * 分层存储：
     * sdsKey 对应高层汇总数据。
     * bitmapKey 对应底层事实数据（分片存储）。
     * aggKey 对应中间层增量数据。
     * 可扩展性： 通过引入 schema 和 metric 等动态参数，支持多种实体类型和指标的灵活扩展。
 * 4. 典型使用场景
     * 点赞/收藏计数：
     * 使用 bitmapKey 存储用户行为的位图数据。
     * 使用 sdsKey 存储最终的汇总计数。
     * 使用 aggKey 存储刷写前的增量变更。
     * 高性能读写：
     * 通过分片机制（chunk）减少单键压力。
     * 通过聚合桶（aggKey）降低刷写频率，提升性能。
 */
public final class CounterKeys {
    private CounterKeys() {}

    /*
    功能：生成 SDS（Summary Data Structure）键，用于存储实体的汇总计数数据。
    命名规则：cnt:{schema}:{entityType}:{entityId}
    cnt：表示计数（count）相关数据。
    schema：来自 CounterSchema.SCHEMA_ID，标识数据结构版本或模式。
    entityType 和 entityId：分别表示实体类型和实体 ID。
    用途：用于快速定位某个实体的汇总计数信息。
    **/
    public static String sdsKey(String entityType, String entityId) {
        return String.format("cnt:%s:%s:%s", CounterSchema.SCHEMA_ID, entityType, entityId); // 固定结构计数（SDS）键
    }

    /*
    功能：生成 位图分片键，用于存储实体在特定分片中的位图数据。
    命名规则：bm:{metric}:{entityType}:{entityId}:{chunk}
    bm：表示位图（bitmap）数据。
    metric：指标名称（如 like、fav）。
    entityType 和 entityId：实体类型和实体 ID。
    chunk：分片编号，来源于 BitmapShard.chunkOf(userId)。
    用途：支持对大规模用户行为数据的分片存储，避免单个键过大。
    **/
    // 分片键：bm:{metric}:{etype}:{eid}:{chunk}
    public static String bitmapKey(String metric, String entityType, String entityId, long chunk) {
        return String.format("bm:%s:%s:%s:%d", metric, entityType, entityId, chunk); // 位图事实层（分片）
    }

    /*
    功能：生成 聚合增量桶键，用于临时存储刷写前的增量数据。
    命名规则：agg:{schema}:{entityType}:{entityId}
    agg：表示聚合（aggregate）数据。
    schema：来自 CounterSchema.SCHEMA_ID。
    entityType 和 entityId：实体类型和实体 ID。
    用途：在数据刷写到 SDS 前，暂存增量变更，确保一致性。
     */

    public static String aggKey(String entityType, String entityId) {
        return String.format("agg:%s:%s:%s", CounterSchema.SCHEMA_ID, entityType, entityId); // 刷写前的增量存储桶
    }
}