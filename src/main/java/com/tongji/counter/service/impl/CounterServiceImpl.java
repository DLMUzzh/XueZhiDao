package com.tongji.counter.service.impl;

import com.tongji.counter.schema.CounterKeys;
import com.tongji.counter.schema.CounterSchema;
import com.tongji.counter.schema.BitmapShard;
import com.tongji.counter.service.CounterService;
import com.tongji.counter.event.CounterEvent;
import com.tongji.counter.event.CounterEventProducer;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.context.ApplicationEventPublisher;
import org.redisson.api.RedissonClient;
import org.redisson.api.RLock;
import org.redisson.api.RRateLimiter;
import org.redisson.api.RateType;
import org.redisson.api.RBucket;
import org.springframework.beans.factory.annotation.Value;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 内容实体计数服务实现（位图事实 + 事件聚合 + SDS 汇总）。
 *
 * <p>职责：</p>
 * - 位图原子切换并产出计数事件（幂等）；
 * - 读取汇总计数（SDS），异常时基于位图分片重建；
 * - 批量读取优化与“是否点赞/收藏”判定。
 */
@Service
public class CounterServiceImpl implements CounterService {

    // Redis 操作模板，用于执行 Redis 命令
    private final StringRedisTemplate redis;
    // Lua 脚本，用于原子性地切换位图状态（点赞/取消点赞、收藏/取消收藏）
    private final DefaultRedisScript<Long> toggleScript;
    // 事件生产者，用于发布计数变更事件（如点赞、取消点赞等）
    private final CounterEventProducer eventProducer;
    // 应用事件发布器，用于发布本地事件（如缓存失效、旁路更新等）
    private final ApplicationEventPublisher eventPublisher;
    // Redisson 客户端，用于分布式锁、限流器等高级功能
    private final RedissonClient redisson;
    // 分布式锁的超时时间（毫秒），默认值为 5000ms
    @Value("${counter.rebuild.lock.ttl-ms:5000}")
    private long lockTtlMs;
    // 限流器每窗口允许的重建次数，默认值为 3 次
    @Value("${counter.rebuild.rate.permits:3}")
    private int ratePermits;
    // 限流器窗口大小（秒），默认值为 10 秒
    @Value("${counter.rebuild.rate.window-seconds:10}")
    private int rateWindowSeconds;
    // 指数退避的基础延迟时间（毫秒），默认值为 500ms
    @Value("${counter.rebuild.backoff.base-ms:500}")
    private long backoffBaseMs;
    // 指数退避的最大延迟时间（毫秒），默认值为 30000ms
    @Value("${counter.rebuild.backoff.max-ms:30000}")
    private long backoffMaxMs;

    public CounterServiceImpl(StringRedisTemplate redis, CounterEventProducer eventProducer, ApplicationEventPublisher eventPublisher, RedissonClient redisson) {
        this.redis = redis;
        this.eventProducer = eventProducer;
        this.eventPublisher = eventPublisher;
        this.redisson = redisson;
        this.toggleScript = new DefaultRedisScript<>();
        this.toggleScript.setResultType(Long.class);
        // 位图状态原子切换，仅在状态变化时返回 1
        this.toggleScript.setScriptText(TOGGLE_LUA);
    }

    /**
     * 点赞：位图原子置位，仅当状态从未点赞→已点赞时返回 true。
     * 同步路径完成事实层更新后产出增量事件，异步聚合到计数快照。
     * @param entityType 实体类型
     * @param entityId 实体 ID
     * @param userId 用户 ID
     * @return 是否发生状态变化（幂等）
     */
    @Override
    public boolean like(String entityType, String entityId, long userId) {
        return toggle(entityType, entityId, userId, "like", CounterSchema.IDX_LIKE, true);
    }

    /**
     * 取消点赞：位图原子清零，仅当状态从已点赞→未点赞时返回 true。
     * 产出增量事件（delta=-1），异步聚合到计数快照。
     */
    @Override
    public boolean unlike(String entityType, String entityId, long userId) {
        return toggle(entityType, entityId, userId, "like", CounterSchema.IDX_LIKE, false);
    }

    /**
     * 收藏：位图原子置位，并产出增量事件（delta=+1）。
     */
    @Override
    public boolean fav(String entityType, String entityId, long userId) {
        return toggle(entityType, entityId, userId, "fav", CounterSchema.IDX_FAV, true);
    }

    /**
     * 取消收藏：位图原子清零，并产出增量事件（delta=-1）。
     */
    @Override
    public boolean unfav(String entityType, String entityId, long userId) {
        return toggle(entityType, entityId, userId, "fav", CounterSchema.IDX_FAV, false);
    }

    /**
     * 位图状态切换：仅在状态变化时返回成功，并产出增量事件。
     * @param etype 实体类型  例如“文章”、“用户”或“评论”等
     * @param eid 实体 ID
     * @param uid 用户 ID
     * @param metric 指标名称（like/fav）
     * @param idx 指标索引（用于 SDS 固定结构定位）
     * @param add 是否置位（true=添加，false=移除）
     */
    private boolean toggle(String etype, String eid, long uid, String metric, int idx, boolean add) {
        // 固定分片定位：按用户ID映射到 chunk 与分片内 bit 偏移，避免单键膨胀与热点
        long chunk = BitmapShard.chunkOf(uid); // 计算用户ID所属的分片编号
        // 分片内位偏移
        long bit = BitmapShard.bitOf(uid); // 计算用户ID在分片内的具体位偏移
        String bmKey = CounterKeys.bitmapKey(metric, etype, eid, chunk); // 生成位图分片的Redis键
        List<String> keys = List.of(bmKey); // 将生成的键放入列表，供Lua脚本使用 keys[0]：bm:fav:knowpost:287871677071757312:0
        List<String> args = List.of(String.valueOf(bit), add ? "add" : "remove"); // 构造Lua脚本参数：位偏移和操作类型
        Long changed = redis.execute(toggleScript, keys, args.toArray()); // 执行Lua脚本，原子性地切换位图状态
        boolean ok = changed == 1L; // 判断是否发生了状态变化（1表示变化，0表示无变化）
        if (ok) {
            int delta = add ? 1 : -1; // 计算计数变化量：新增为+1，删除为-1
            // 产出计数事件（异步聚合），分区按实体维度保证同实体事件顺序
            eventProducer.publish(CounterEvent.of(etype, eid, metric, idx, uid, delta)); // 发布计数变更事件

            // 本地事件：触发缓存失效/旁路更新等快速路径       用于知文的feed三级缓存中
            // 监听者 FeedCacheInvalidationListener @EventListener onCounterChanged
            eventPublisher.publishEvent(CounterEvent.of(etype, eid, metric, idx, uid, delta)); // 发布本地事件，用于缓存更新等
        }
        return ok; // 返回是否成功切换状态
    }

    /**
     * 获取实体计数汇总（SDS）。
     * 若缺失或结构异常则触发基于位图的事实重建，并清理对应聚合字段。
     * counterService.getCounts("knowpost", String.valueOf(id), List.of("like","fav"));
     */
    @Override
    public Map<String, Long> getCounts(String entityType, String entityId, List<String> metrics) {
        // 构建 SDS（Summary Data Structure）的 Redis 键   cnt:{schema}:{entityType}:{entityId}
        String sdsKey = CounterKeys.sdsKey(entityType, entityId); // cnt:v1:knowpost:287871677071757312
        // 计算预期的 SDS 字节长度：字段数量 × 每个字段占用的字节数（这里是 4 字节）
        int expectedLen = CounterSchema.SCHEMA_LEN * CounterSchema.FIELD_SIZE;
        // 从 Redis 中读取原始的 SDS 数据
        byte[] raw = getRaw(sdsKey);
        // 判断是否需要重建 SDS：
        // 如果数据不存在或者长度不符合预期，则需要重建
        boolean needRebuild = (raw == null || raw.length != expectedLen);
        // 创建一个有序的结果映射，用于存储每个指标及其对应的计数值
        Map<String, Long> result = new LinkedHashMap<>();
        // 如果需要重建 SDS
        if (needRebuild) {
            // 检查当前是否处于指数退避期（即短时间内不允许频繁重建）
            if (inBackoff(entityType, entityId)) {
                // 如果在退避期内，直接返回所有指标为 0 的结果
                for (String m : metrics) {
                    result.put(m, 0L);
                }
                return result;
            }
            // 检查是否超过限流阈值（防止重建风暴）
            if (!allowedByRateLimiter(entityType, entityId)) {
                // 如果超过了限流阈值，增加退避等级并返回所有指标为 0 的结果
                escalateBackoff(entityType, entityId);
                for (String m : metrics) {
                    result.put(m, 0L);
                }
                return result;
            }
            // 构建分布式锁的键名
            String lockKey = String.format("lock:sds-rebuild:%s:%s", entityType, entityId);
            // 获取 Redisson 分布式锁实例
            RLock lock = redisson.getLock(lockKey);
            boolean locked = false;
            try {
                // 尝试获取锁，立即返回结果（非阻塞）
                locked = lock.tryLock(0L, TimeUnit.MILLISECONDS);
                // 如果未能获取锁，说明有其他线程正在重建，进入退避状态并返回默认值
                if (!locked) {
                    escalateBackoff(entityType, entityId);
                    for (String m : metrics) {
                        result.put(m, 0L);
                    }
                    return result;
                }
                // 成功获取锁后开始重建 SDS
                byte[] newSds = new byte[expectedLen]; // 新建一个空的 SDS 结构
                List<String> rebuildFields = new ArrayList<>(); // 存储需要重建的字段索引
                // 遍历请求中的指标列表
                for (String m : metrics) {
                    // 获取该指标在 SDS 中的位置索引
                    Integer idx = CounterSchema.NAME_TO_IDX.get(m);
                    if (idx == null) {
                        continue; // 忽略不支持的指标
                    }
                    // 统计该指标在位图分片中的实际计数
                    long sum = bitCountShardsPipelined(m, entityType, entityId);
                    // 将统计结果写入新的 SDS 结构中（大端序）
                    writeInt32BE(newSds, idx * CounterSchema.FIELD_SIZE, sum);
                    // 将结果放入返回映射中
                    result.put(m, sum);
                    // 记录需要重建的字段索引
                    rebuildFields.add(String.valueOf(idx));
                }
                // 将新构建的 SDS 写回到 Redis 中
                setRaw(sdsKey, newSds);
                // 清理聚合桶中旧的数据，避免重复计算
                if (!rebuildFields.isEmpty()) {
                    String aggKey = CounterKeys.aggKey(entityType, entityId);
                    redis.opsForHash().delete(aggKey, rebuildFields.toArray());
                }
                // 重置退避状态（因为本次重建成功）
                resetBackoff(entityType, entityId);
            } catch (InterruptedException ie) {
                // 处理中断异常：恢复线程中断状态，并进入退避状态
                Thread.currentThread().interrupt();
                escalateBackoff(entityType, entityId);
                for (String m : metrics) {
                    result.put(m, 0L);
                }
                return result;
            } finally {
                // 确保释放锁（即使出现异常也要释放）
                if (locked) {
                    try {
                        lock.unlock();
                    } catch (Exception ignore) {}
                }
            }
        } else {
            // 如果不需要重建，直接从现有 SDS 中读取数据
            for (String m : metrics) {
                // 获取指标在 SDS 中的索引
                Integer idx = CounterSchema.NAME_TO_IDX.get(m);
                if (idx == null) continue; // 忽略不支持的指标
                // 计算该指标在 SDS 中的偏移量
                int off = idx * CounterSchema.FIELD_SIZE;
                // 从 SDS 中读取该指标的值（大端序）
                long val = readInt32BE(raw, off);
                // 将读取到的值放入结果映射中
                result.put(m, val);
            }
        }
        // 返回最终的计数结果
        return result;
    }


    /**
     * 批量获取实体计数（管道批量 GET 降低 RTT）。
     * 缺失或结构异常（长度不符）时按零返回，保证接口稳定。
     * @param entityType 实体类型
     * @param entityIds 实体ID列表
     * @param metrics 指标名列表
     * @return 每个实体的指标计数映射
     */
    @Override
    public Map<String, Map<String, Long>> getCountsBatch(String entityType, List<String> entityIds, List<String> metrics) {
        Map<String, Map<String, Long>> out = new LinkedHashMap<>();
        if (entityIds == null || entityIds.isEmpty() || metrics == null || metrics.isEmpty()) {
            return out;
        }

        List<String> keys = new ArrayList<>(entityIds.size());
        for (String eid : entityIds) {
            keys.add(CounterKeys.sdsKey(entityType, eid));
        }

        // 管道批量 GET：将多个 SDS 读取合并到一次往返
        List<Object> raws = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String k : keys) {
                connection.stringCommands().get(k.getBytes(StandardCharsets.UTF_8));
            }
            return null;
        });

        int expectedLen = CounterSchema.SCHEMA_LEN * CounterSchema.FIELD_SIZE;
        for (int i = 0; i < entityIds.size(); i++) {
            String eid = entityIds.get(i);
            Object rawObj = i < raws.size() ? raws.get(i) : null;
            byte[] raw = (rawObj instanceof byte[]) ? (byte[]) rawObj : null;

            Map<String, Long> m = new LinkedHashMap<>();
            if (raw != null && raw.length == expectedLen) {
                for (String name : metrics) {
                    Integer idx = CounterSchema.NAME_TO_IDX.get(name);
                    if (idx == null) continue;
                    int off = idx * CounterSchema.FIELD_SIZE;
                    long val = readInt32BE(raw, off);
                    m.put(name, val);
                }
            } else {
                for (String name : metrics) {
                    m.put(name, 0L); // 缺失或异常结构时补零，避免接口失败与重建风暴
                }
            }
            out.put(eid, m);
        }
        return out;
    }

    /**
     * 是否点赞判定：基于分片位图在分片内做位测试。
     * 毫秒级读取，不依赖计数快照。
     */
    @Override
    public boolean isLiked(String entityType, String entityId, long userId) {
        long chunk = BitmapShard.chunkOf(userId);
        long bit = BitmapShard.bitOf(userId);
        return getBit(CounterKeys.bitmapKey("like", entityType, entityId, chunk), bit);
    }

    /**
     * 是否收藏判定：同点赞，基于分片位图位测试。
     */
    @Override
    public boolean isFaved(String entityType, String entityId, long userId) {
        long chunk = BitmapShard.chunkOf(userId);
        long bit = BitmapShard.bitOf(userId);
        return getBit(CounterKeys.bitmapKey("fav", entityType, entityId, chunk), bit);
    }

    /**
     * 读取位图某偏移位（GETBIT）。
     * @param key 位图分片键
     * @param offset 分片内位偏移
     * @return 位是否为 1
     */
    private boolean getBit(String key, long offset) {
        Boolean bit = redis.execute((RedisCallback<Boolean>) connection ->
                connection.stringCommands().getBit(key.getBytes(StandardCharsets.UTF_8), offset));
        return Boolean.TRUE.equals(bit);
    }

    /**
     * 读取 SDS 原始字节（固定结构，长度=字段数×4）。
     * 通过 Redis 的 GET 命令，从指定的键中读取原始数据，并将其作为字节数组返回
     */
    private byte[] getRaw(String key) {
        return redis.execute((RedisCallback<byte[]>) connection ->
                connection.stringCommands().get(key.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * 写入 SDS 原始字节（覆盖式写）。
     */
    private void setRaw(String key, byte[] val) {
        redis.execute((RedisCallback<Void>) connection -> {
            connection.stringCommands().set(key.getBytes(StandardCharsets.UTF_8), val);
            return null;
        });
    }

    /**
     * 是否处于指数退避期：期间跳过重建并返回降级结果。
     */
    private boolean inBackoff(String entityType, String entityId) {
        String bKey = String.format("backoff:sds-rebuild:until:%s:%s", entityType, entityId);
        //使用 Redisson 客户端获取一个 RBucket 对象，用于操作 Redis 中的键值对。
        //bucket.get() 从 Redis 中读取该键对应的值（即退避截止时间戳），类型为 Long。
        RBucket<Long> bucket = redisson.getBucket(bKey);
        Long until = bucket.get();
        //如果两个条件都满足，返回 true，表示该实体正处于退避期；否则返回 false。
        return until != null && System.currentTimeMillis() < until;
    }

    /**
     * 增加退避级别并设置下次允许尝试的时间（指数递增，封顶）。
     * 该方法用于实现指数退避算法，通过增加退避级别来控制重试频率，避免系统过载。
     *
     * @param entityType 实体类型，用于标识不同的退避记录
     * @param entityId 实体ID，用于标识具体的实体
     */
    private void escalateBackoff(String entityType, String entityId) {
        // 构建存储当前退避级别的Redis键
        String eKey = String.format("backoff:sds-rebuild:exp:%s:%s", entityType, entityId);
        // 构建存储下次允许尝试时间的Redis键
        String uKey = String.format("backoff:sds-rebuild:until:%s:%s", entityType, entityId);

        // 获取Redis中的退避级别计数器
        RBucket<Integer> expB = redisson.getBucket(eKey);
        // 获取Redis中的下次允许尝试时间
        RBucket<Long> untilB = redisson.getBucket(uKey);
        // 获取当前退避级别
        Integer exp = expB.get();

        // 计算下一个退避级别，如果当前为null则从0开始，最大不超过10
        int nextExp = Math.min(exp == null ? 0 : exp + 1, 10);
        // 计算延迟时间，使用指数递增算法，但不超过最大延迟时间 2的nextE次方
        long delay = Math.min(backoffBaseMs * (1L << nextExp), backoffMaxMs);
        // 计算下次允许尝试的时间戳
        long until = System.currentTimeMillis() + delay;

        // 设置过期时间，避免长时间残留
        expB.set(nextExp);
        untilB.set(until, Duration.ofMillis(delay + 1000));
    }

    /**
     * 重置退避状态（成功重建后）。
     */
    private void resetBackoff(String entityType, String entityId) {
        String eKey = String.format("backoff:sds-rebuild:exp:%s:%s", entityType, entityId);
        String uKey = String.format("backoff:sds-rebuild:until:%s:%s", entityType, entityId);

        try {
            redisson.getBucket(eKey).delete();
        } catch (Exception ignore) {}

        try {
            redisson.getBucket(uKey).delete();
        } catch (Exception ignore) {}
    }

    /**
     * 限流判断：单位窗口可重建次数，防止抖动与风暴。
 * 该方法使用Redisson实现基于令牌桶算法的限流功能，控制特定实体的重建频率。
     */
    private boolean allowedByRateLimiter(String entityType, String entityId) {
    // 根据实体类型和ID生成唯一的限流器键名，确保每个实体的限流规则独立
        String rlKey = String.format("rl:sds-rebuild:%s:%s", entityType, entityId);
        //通过 Redisson 客户端获取一个限流器实例，用于管理该实体的重建频率。
        // Redisson的RRateLimiter提供了分布式环境下的限流功能
        RRateLimiter limiter = redisson.getRateLimiter(rlKey);
        // 初始化速率（如已存在则忽略）
        // 设置限流器的参数：使用整体限流模式(OVERALL)
        // ratePermits表示每rateWindowSeconds秒内可获取的令牌数   限流器每窗口允许的重建次数，默认值为 3 次
        // Duration.ofSeconds(rateWindowSeconds)定义了时间窗口大小   限流器窗口大小（秒），默认值为 10 秒
        limiter.trySetRate(RateType.OVERALL, ratePermits, Duration.ofSeconds(rateWindowSeconds));
        // 再次设置相同的参数，可能是为了确保限流器配置的正确性
        // 在实际应用中，这两行可能是冗余的，具体取决于业务需求
        limiter.trySetRate(RateType.OVERALL, ratePermits, Duration.ofSeconds(rateWindowSeconds));
        // 尝试获取一个令牌，返回是否成功获取
        // 如果获取成功，说明当前请求在限流范围内，允许执行
        // 如果获取失败，说明超出限流阈值，需要拒绝执行
        return limiter.tryAcquire(1);
    }

    /**
     * 以大端序读取 32 位无符号整型。
     */
    private static long readInt32BE(byte[] buf, int off) {
        long n = 0;
        for (int i = 0; i < 4; i++) {
            n = (n << 8) | (buf[off + i] & 0xFFL);
        }
        return n;
    }

    /**
     * 以大端序写入 32 位无符号整型（截断到 0~2^32-1）。
     */
    private static void writeInt32BE(byte[] buf, int off, long val) {
        long n = Math.max(0, Math.min(val, 0xFFFF_FFFFL));
        buf[off] = (byte) ((n >>> 24) & 0xFF);
        buf[off + 1] = (byte) ((n >>> 16) & 0xFF);
        buf[off + 2] = (byte) ((n >>> 8) & 0xFF);
        buf[off + 3] = (byte) (n & 0xFF);
    }

    /**
     该方法的核心流程如下：
     根据实体信息构造 Redis 键的匹配模式。
     枚举所有匹配的位图分片键。
     使用管道批量执行 BITCOUNT 命令，统计每个分片中的有效位数。
     累加所有分片的统计结果，返回总计数。
     这种方法适用于需要从底层位图数据重建高层计数（SDS）的场景，兼顾了性能和准确性。
     */
    private long bitCountShardsPipelined(String metric, String etype, String eid) {
        //bm:fav:knowpost:287871677071757312:0     大key存在分片        bm:{metric}:{etype}:{eid}:{chunk}
        String pattern = String.format("bm:%s:%s:%s:*", metric, etype, eid);
        // 返回所有符合模式的key
        Set<String> keys = redis.keys(pattern); 
        if (keys.isEmpty()) return 0L;

        // 管道批量 BITCOUNT 汇总
        //使用 Redis 管道（Pipeline）批量执行 BITCOUNT 命令，减少网络往返次数（RTT）。
        //对每个匹配的键 k，调用 BITCOUNT 统计其中被置为 1 的位数（即点赞/收藏的数量）。
        //结果存储在 res 列表中，每个元素对应一个键的统计值。
        List<Object> res = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String k : keys) {
                connection.stringCommands().bitCount(k.getBytes(StandardCharsets.UTF_8));
            }
            return null;
        });
        long sum = 0L;

        for (Object o : res) {
            if (o instanceof Number n) {
                sum += n.longValue();
            }
        }
        return sum;
    }

    // Redis 内嵌 Lua（Redis 5/6 的 Lua 5.1），位图原子切换（分片内偏移）
    //将位图相应位置至0或1
    private static final String TOGGLE_LUA = """
            local bmKey = KEYS[1]
            local offset = tonumber(ARGV[1])
            local op = ARGV[2] -- 'add' or 'remove'
            local prev = redis.call('GETBIT', bmKey, offset)
            if op == 'add' then
              if prev == 1 then return 0 end
              redis.call('SETBIT', bmKey, offset, 1)
              return 1
            elseif op == 'remove' then
              if prev == 0 then return 0 end
              redis.call('SETBIT', bmKey, offset, 0)
              return 1
            end
            return -1
            """;
}
