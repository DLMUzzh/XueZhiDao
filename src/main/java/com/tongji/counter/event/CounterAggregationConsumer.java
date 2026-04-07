package com.tongji.counter.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tongji.counter.schema.CounterKeys;
import com.tongji.counter.schema.CounterSchema;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.List;

/**
 * 计数事件聚合与刷写消费者。
 *
 * <p>职责：</p>
 * - 消费点赞/收藏等增量事件，写入 Redis 聚合桶（Hash）；
 * - 以固定延迟定时任务将聚合增量折叠到 SDS 固定结构计数；
 * - 刷写成功后删除聚合字段，避免重复加算。
 *
 * 聚合桶（aggKey）在 CounterAggregationConsumer 类中的作用如下：
 * 临时存储增量数据
 * 聚合桶使用 Redis 的 Hash 结构，键名为 agg:{schema}:{etype}:{eid}，用于临时存储来自 Kafka 的计数事件增量（delta）。
 * 每个字段（field）对应一个指标索引（如 like、fav），值为该指标的增量。
 * 实现批量聚合
 * 通过将多个事件的增量先写入聚合桶，再定时刷写到最终的 SDS 固定结构计数中，减少对 Redis 的频繁写操作，提升性能。
 * 支持容错与重试
 * 如果刷写失败，聚合桶中的数据不会丢失，可以在下一次定时任务中重新尝试刷写，确保数据一致性。
 * 降低 Redis 键空间噪音
 * 刷写成功后，聚合桶中对应的字段会被删除；若整个聚合桶为空，则删除该键，避免无效扫描和资源浪费。
 * 总结：聚合桶是实现高效、可靠计数聚合的核心中间层，兼顾性能优化与数据一致性。
 */
@Service
public class CounterAggregationConsumer {

    private final ObjectMapper objectMapper;
    private final StringRedisTemplate redis;
    //封装 Redis 的 Lua 脚本
    private final DefaultRedisScript<Long> incrScript;

    // 使用 Redis Hash 作为持久化聚合桶：agg:{schema}:{etype}:{eid} ，field=idx ，value=delta
    public CounterAggregationConsumer(ObjectMapper objectMapper, StringRedisTemplate redis) {
        this.objectMapper = objectMapper;
        this.redis = redis;
        this.incrScript = new DefaultRedisScript<>();
        this.incrScript.setResultType(Long.class);
        this.incrScript.setScriptText(INCR_FIELD_LUA); // 原子将增量折叠到 SDS 指定段（大端 32 位）
    }

    /**
     * 消费计数事件并写入聚合桶。
     * @param message 事件 JSON
     * @param ack 位点确认对象（手动提交）
     */
    @KafkaListener(topics = CounterTopics.EVENTS, groupId = "counter-agg")   //counter-agg
    public void onMessage(String message, Acknowledgment ack) throws Exception {
        System.out.println("counter-agg收到消息");
        CounterEvent evt = objectMapper.readValue(message, CounterEvent.class);
        String aggKey = CounterKeys.aggKey(evt.getEntityType(), evt.getEntityId());
        String field = String.valueOf(evt.getIdx());
        try {
            // 将增量持久化到 Redis Hash
            redis.opsForHash().increment(aggKey, field, evt.getDelta());
            // 成功后提交位点，绑定“已持久化”语义
            ack.acknowledge();
        } catch (Exception ex) {
            // 不提交位点以便重试
        }
    }

    /**
     * 定时任务：将 Redis 聚合桶中的增量数据刷写到 SDS 固定结构计数中。
     * 该方法每 1 秒执行一次，确保数据的最终一致性。
     */
    @Scheduled(fixedDelay = 1000L)
    public void flush() {
        // 扫描所有以 "agg:{schema}:" 开头的聚合桶键（简化实现，生产环境中建议使用索引集合优化）
        Set<String> keys = redis.keys("agg:" + CounterSchema.SCHEMA_ID + ":*");
        if (keys.isEmpty()) {return;}
        // 遍历所有聚合桶键
        for (String aggKey : keys) {
            // 获取当前聚合桶的所有字段及其值（field -> delta）
            Map<Object, Object> entries = redis.opsForHash().entries(aggKey);
            if (entries.isEmpty()) {continue;}
            // 解析聚合桶键，提取实体类型（etype）和实体 ID（eid）
            // 格式为：agg:schema:etype:eid
            String[] parts = aggKey.split(":", 4);
            // 如果格式不符合预期，跳过处理
            if (parts.length < 4) {continue;}
            // 构造 SDS 计数 Key，用于存储最终的计数值
            String cntKey = CounterKeys.sdsKey(parts[2], parts[3]);
            // 遍历聚合桶中的每个字段（指标索引）和对应的增量值
            for (Map.Entry<Object, Object> e : entries.entrySet()) {
                String field = String.valueOf(e.getKey()); // 字段名（即指标索引）
                long delta; // 增量值
                try {
                    // 将字段值解析为长整型增量
                    delta = Long.parseLong(String.valueOf(e.getValue()));
                } catch (NumberFormatException nfe) {
                    // 如果解析失败，跳过该字段
                    continue;
                }
                if (delta == 0) {
                    // 如果增量为 0，无需处理，跳过
                    continue;
                }
                int idx; // 指标索引
                try {
                    // 将字段名解析为整型索引
                    idx = Integer.parseInt(field);
                } catch (NumberFormatException nfe) {
                    // 如果解析失败，跳过该字段
                    continue;
                }

                try {
                    // 执行 Lua 脚本，将增量原子性地写入 SDS 计数结构
                    redis.execute(incrScript, List.of(cntKey),
                            String.valueOf(CounterSchema.SCHEMA_LEN),
                            String.valueOf(CounterSchema.FIELD_SIZE),
                            String.valueOf(idx),
                            String.valueOf(delta));
                    // 成功后删除该字段，避免重复加算
                    redis.opsForHash().delete(aggKey, field);
                } catch (Exception ex) {
                    // 如果执行失败，保留字段以便下一轮重试
                }
            }
            // 检查聚合桶是否已清空
            Long size = redis.opsForHash().size(aggKey);
            if (size == 0L) {
                // 如果聚合桶已无字段，删除该键以降低 Redis 键空间噪音
                redis.delete(aggKey);
            }
        }
    }


    private static final String INCR_FIELD_LUA = """
            
            local cntKey = KEYS[1]
            local schemaLen = tonumber(ARGV[1])
            local fieldSize = tonumber(ARGV[2]) -- 固定为4
            local idx = tonumber(ARGV[3])
            local delta = tonumber(ARGV[4])
            
            local function read32be(s, off)
              local b = {string.byte(s, off+1, off+4)}
              local n = 0
              for i=1,4 do n = n * 256 + b[i] end
              return n
            end
            
            local function write32be(n)
              local t = {}
              for i=4,1,-1 do t[i] = n % 256; n = math.floor(n/256) end
              return string.char(unpack(t))
            end
            
            local cnt = redis.call('GET', cntKey)
            if not cnt then cnt = string.rep(string.char(0), schemaLen * fieldSize) end
            local off = idx * fieldSize
            local v = read32be(cnt, off) + delta
            if v < 0 then v = 0 end
            local seg = write32be(v)
            cnt = string.sub(cnt, 1, off) .. seg .. string.sub(cnt, off+fieldSize+1)
            redis.call('SET', cntKey, cnt)
            return 1
            """;
}