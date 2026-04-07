package com.tongji.relation.service.impl;

import com.tongji.relation.mapper.RelationMapper;
import com.tongji.relation.service.RelationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tongji.relation.event.RelationEvent;
import com.tongji.relation.outbox.OutboxMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.tongji.user.mapper.UserMapper;
import com.tongji.user.domain.User;
import com.tongji.profile.api.dto.ProfileResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.nio.charset.StandardCharsets;
import org.springframework.data.redis.core.RedisCallback;

/**
 * 关系服务实现。
 * 设计要点：
 * - 写路径：关注/取消关注经 Lua 令牌桶限流后入库，并以 Outbox 事件异步驱动粉丝表更新与缓存维护；
 * - 读路径：优先读取 Redis ZSet（关注/粉丝）并按需回填，支持偏移与游标两种分页；大V用户启用本地 Top 缓存；
 * - 计数：用户维度计数（关注/粉丝等）通过独立服务维护，阈值判断如“大V”基于 SDS 段值；
 * - 并发与一致性：回填后设置短 TTL，降低陈旧风险；Outbox 事件消费者提供幂等与去重保障。
 */
@Service
public class RelationServiceImpl implements RelationService {
    private final RelationMapper mapper;
    private final OutboxMapper outboxMapper;
    private final StringRedisTemplate redis;
    private final DefaultRedisScript<Long> tokenScript;
    //objectMapper 是 Jackson 提供的工具类，用于处理 JSON 数据的序列化和反序列化，在你的项目中主要用于事件消息的持久化操作。
    private final ObjectMapper objectMapper;
    private final Cache<Long, List<Long>> flwsTopCache;
    private final Cache<Long, List<Long>> fansTopCache;
    private final UserMapper userMapper;
    

    /**
     * 关系服务实现构造函数。
     * @param mapper 关系表数据访问
     * @param outboxMapper Outbox 事件写入访问
     * @param redis Redis 客户端
     * @param objectMapper JSON 序列化器
     */
    public RelationServiceImpl(RelationMapper mapper,
                               OutboxMapper outboxMapper,
                               StringRedisTemplate redis,
                               ObjectMapper objectMapper,
                               UserMapper userMapper) {
        this.mapper = mapper;
        this.outboxMapper = outboxMapper;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.tokenScript = new DefaultRedisScript<>();
        this.tokenScript.setResultType(Long.class);
        this.tokenScript.setScriptText(TOKEN_BUCKET_LUA);
        this.flwsTopCache = Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(Duration.ofMinutes(10)).build();
        this.fansTopCache = Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(Duration.ofMinutes(10)).build();
        this.userMapper = userMapper;
    }

    /**
     * 关注操作，限流通过令牌桶，并写入 Outbox 以异步构建缓存与粉丝表。
     * @param fromUserId 发起关注的用户ID
     * @param toUserId 被关注的用户ID
     * @return 是否关注成功
     */
    @Override
    @Transactional
    public boolean follow(long fromUserId, long toUserId) {
        // Lua 脚本令牌桶限流
        Long ok = redis.execute(tokenScript, List.of("rl:follow:" + fromUserId), "100", "1");
        if (ok == null || ok == 0L) {
            return false;
        }

        long id = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        int inserted = mapper.insertFollowing(id, fromUserId, toUserId, 1);

        if (inserted > 0) {
            try {
                Long outId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
                String payload = objectMapper.writeValueAsString(new RelationEvent("FollowCreated", fromUserId, toUserId, id));
                outboxMapper.insert(outId, "following", id, "FollowCreated", payload);
            } catch (Exception ignored) {}

            return true;
        }
        return false;
    }

    /**
     * 取消关注操作，并写入 Outbox 事件。
     * @param fromUserId 发起取消关注的用户ID
     * @param toUserId 被取消关注的用户ID
     * @return 是否取消成功
     */
    @Override
    @Transactional
    public boolean unfollow(long fromUserId, long toUserId) {
        int updated = mapper.cancelFollowing(fromUserId, toUserId);
        if (updated > 0) {
            try {
                Long outId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
                String payload = objectMapper.writeValueAsString(new RelationEvent("FollowCanceled", fromUserId, toUserId, null));
                outboxMapper.insert(outId, "following", null, "FollowCanceled", payload);
            } catch (Exception ignored) {}
            return true;
        }
        return false;
    }

    /**
     * 判断是否已关注。
     * @param fromUserId 关注发起者
     * @param toUserId 被关注者
     * @return 是否已关注
     */
    @Override
    public boolean isFollowing(long fromUserId, long toUserId) {
        return mapper.existsFollowing(fromUserId, toUserId) > 0;
    }

    /**
     * 获取关注列表（偏移分页），优先读取 Redis ZSet，未命中时回填并设置 TTL。
     * @param userId 用户ID
     * @param limit 返回数量上限
     * @param offset 偏移量
     * @return 关注的用户ID列表
     */
    @Override
    public List<Long> following(long userId, int limit, int offset) {
        String key = "uf:flws:" + userId;
        return getListWithOffset(
                key,
                offset,
                limit,
                need -> mapper.listFollowingRows(userId, need, 0),
                "toUserId",
                "createdAt",
                flwsTopCache,
                userId
        );
    }

    /**
     * 获取粉丝列表（偏移分页），ZSet 优先，DB 回填并设置 TTL。
     * @param userId 用户ID
     * @param limit 返回数量上限
     * @param offset 偏移量
     * @return 粉丝用户ID列表
     */
    @Override
    public List<Long> followers(long userId, int limit, int offset) {
        String key = "uf:fans:" + userId;
        return getListWithOffset(
                key,
                offset,
                limit,
                need -> mapper.listFollowerRows(userId, need, 0),
                "fromUserId",
                "createdAt",
                fansTopCache,
                userId
        );
    }

    /**
     * 查询双方关系状态。
     * @param userId 当前用户ID
     * @param otherUserId 对方用户ID
     * @return 三态关系：following/followedBy/mutual
     */
    @Override
    public Map<String, Boolean> relationStatus(long userId, long otherUserId) {
        boolean following = isFollowing(userId, otherUserId);
        boolean followedBy = isFollowing(otherUserId, userId);
        boolean mutual = following && followedBy;
        Map<String, Boolean> m = new LinkedHashMap<>();
        m.put("following", following);
        m.put("followedBy", followedBy);
        m.put("mutual", mutual);
        return m;
    }

    /**
     * 游标分页获取关注列表，按创建时间倒序基于 ZSet 分数。
     * @param userId 用户ID
     * @param limit 返回数量上限
     * @param cursor 上一页末条的分数（毫秒时间戳），为空代表第一页
     * @return 关注的用户ID列表
     */
    @Override
    public List<Long> followingCursor(long userId, int limit, Long cursor) {
        String key = "uf:flws:" + userId;
        return getListWithCursor(
                key,
                limit,
                cursor,
                // 列出关注行用于缓存回填（包含 createdAt）。
                need -> mapper.listFollowingRows(userId, need, 0),
                "toUserId",
                "createdAt"
        );
    }

    /**
     * 游标分页获取粉丝列表。
     * @param userId 用户ID
     * @param limit 返回数量上限
     * @param cursor 上一页末条的分数（毫秒时间戳），为空代表第一页
     * @return 粉丝用户ID列表
     */
    @Override
    public List<Long> followersCursor(long userId, int limit, Long cursor) {
        String key = "uf:fans:" + userId;
        return getListWithCursor(
                key,
                limit,
                cursor,
                need -> mapper.listFollowerRows(userId, need, 0),
                "fromUserId",
                "createdAt"
        );
    }

    /**
     * 获取用户的关注者资料列表，支持分页查询。
     *
     * @param userId  当前用户的ID，用于查询其关注列表。
     * @param limit   每页返回的最大记录数。
     * @param offset  偏移量，用于跳过前面的记录（仅在非游标分页时生效）。
     * @param cursor  游标分页的起始位置（毫秒时间戳），为空时表示第一页。
     * @return        返回关注者的资料列表（ProfileResponse），按分页规则排序。
     * 游标（Cursor）
     * 基于排序字段（如时间戳、ID等）进行分页。
     * 下一页的起点由上一页最后一条记录的排序字段值决定。
     * 示例：cursor = 上一页最后一条记录的时间戳。
     * 偏移量（Offset）
     * 基于固定的位置偏移进行分页。
     * 下一页的起点由当前页码和每页大小计算得出。
     * 示例：offset = 当前页码 × 每页大小。
     */
    @Override
    public List<ProfileResponse> followingProfiles(long userId, int limit, int offset, Long cursor) {
        // 根据是否提供游标决定使用哪种分页方式：
        // - 如果提供了游标，则调用 followingCursor 方法进行游标分页；
        // - 否则调用 following 方法进行偏移分页。

        List<Long> ids = cursor != null ? followingCursor(userId, limit, cursor)
                : following(userId, limit, offset);
        // 将获取到的用户ID列表转换为对应的用户资料列表（ProfileResponse）。
        return toProfiles(ids);
    }

    @Override
    public List<ProfileResponse> followersProfiles(long userId, int limit, int offset, Long cursor) {
        List<Long> ids = cursor != null ? followersCursor(userId, limit, cursor)
                                        : followers(userId, limit, offset);
        return toProfiles(ids);
    }

    /**
     * 将用户 ID 列表映射为资料视图列表（批量查询并保持输入顺序）。
     */
    private List<ProfileResponse> toProfiles(List<Long> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        List<User> users = userMapper.listByIds(ids);
        Map<Long, User> m = new LinkedHashMap<>(users.size());
        for (User u : users) m.put(u.getId(), u);
        List<ProfileResponse> out = new ArrayList<>(ids.size());
        for (Long id : ids) {
            User u = m.get(id);
            if (u == null) continue;
            out.add(new ProfileResponse(u.getId(), u.getNickname(), u.getAvatar(), u.getBio(), u.getZgId(), u.getGender(), u.getBirthday(), u.getSchool(), u.getPhone(), u.getEmail(), u.getTagsJson()));
        }
        return out;
    }

    /**
     * 判断是否为大V（基于 followers 计数阈值）。
     * @param userId 用户ID
     * @return 是否为大V
     */
    private boolean isBigV(long userId) {
        byte[] raw = redis.execute((RedisCallback<byte[]>) c -> c.stringCommands().get(("ucnt:" + userId).getBytes(StandardCharsets.UTF_8)));
        if (raw == null || raw.length < 20) return false;
        long n = 0;
        int off = 2 * 4;
        for (int i = 0; i < 4; i++) n = (n << 8) | (raw[off + i] & 0xFFL);
        return n >= 500_000L;
    }

    /**
     * 偏移分页读取：优先命中 Redis ZSet 缓存，未命中时从数据库回填并设置 TTL；
     * 大 V 用户维护本地 Top 缓存以降低冷启动开销。
     *
     * @param key           Redis 中 ZSet 的键名，用于存储用户关系数据（如关注/粉丝列表）。
     * @param offset        偏移量，表示从第几条记录开始读取。
     * @param limit         每次查询返回的最大记录数。
     * @param rowsFetcher   函数接口，用于从数据库中获取原始行数据。
     * @param idField       数据库行中表示用户 ID 的字段名。
     * @param tsField       数据库行中表示创建时间的字段名。
     * @param localCache    本地缓存（Caffeine），用于存储大 V 用户的 Top 数据。
     * @param userId        当前用户的 ID，用于判断是否为大 V 用户。
     * @return              返回符合条件的用户 ID 列表。
     */
    private List<Long> getListWithOffset(
            String key,
            int offset,
            int limit,
            IntFunction<Map<Long, Map<String, Object>>> rowsFetcher,
            String idField,
            String tsField,
            Cache<Long, List<Long>> localCache,
            long userId
    ) {
        // 1. 尝试从 Redis ZSet 缓存中读取数据，范围是 [offset, offset + limit - 1]。
        Set<String> cached = redis.opsForZSet().reverseRange(key, offset, offset + limit - 1L);
        if (cached != null && !cached.isEmpty()) {
            // 如果缓存中有数据，直接转换为长整型列表并返回。
            return toLongList(cached);
        }
        // 2. 如果 Redis 缓存未命中，尝试从本地 Top 缓存中读取数据（仅适用于大 V 用户）。
        List<Long> top = localCache != null ? localCache.getIfPresent(userId) : null;
        if (top != null && !top.isEmpty()) {
            // 计算子列表的起始和结束位置，确保不越界。
            int from = Math.min(offset, top.size());
            int to = Math.min(offset + limit, top.size());
            // 返回本地缓存中的子列表。
            return new ArrayList<>(top.subList(from, to));
        }
        // 3. 如果本地缓存也未命中，从数据库中获取数据。
        // 计算需要从数据库中获取的记录数：至少获取 limit + offset 条记录。
        int need = Math.max(1, limit + offset);
        // 调用 rowsFetcher 从数据库中获取原始行数据，最多获取 1000 条以防数据量过大。
        Map<Long, Map<String, Object>> rows = rowsFetcher.apply(Math.min(need, 1000));
        if (rows != null && !rows.isEmpty()) {
            // 将数据库中的行数据填充到 Redis ZSet 中。
            fillZSet(key, rows, idField, tsField, null);
            // 设置 ZSet 的过期时间为 2 小时，避免长期占用内存。
            redis.expire(key, Duration.ofHours(2));

            // 如果是大 V 用户，更新本地 Top 缓存。
            if (localCache != null && isBigV(userId)) {
                maybeUpdateTopCache(userId, key, localCache);
            }

            // 再次从 Redis ZSet 中读取数据。
            Set<String> filled = redis.opsForZSet().reverseRange(key, offset, offset + limit - 1L);
            // 如果读取到数据，转换为长整型列表并返回；否则返回空列表。
            return filled == null ? Collections.emptyList() : toLongList(filled);
        }

        // 4. 如果数据库也没有数据，返回空列表。
        return Collections.emptyList();
    }

    /**
     * 游标分页读取：按分数（毫秒时间戳）倒序读取；未命中时回填满足所需范围的数据并继续读取。
     *
     * @param key          Redis 中 ZSet 的键名，用于存储用户关系数据（如关注/粉丝列表）。
     * @param limit        每次查询返回的最大记录数。
     * @param cursor       游标值（毫秒时间戳），表示上一页最后一条记录的分数。为空时表示第一页。
     * @param rowsFetcher  函数接口，用于从数据库中获取原始行数据。
     * @param idField      数据库行中表示用户 ID 的字段名。
     * @param tsField      数据库行中表示创建时间的字段名。
     * @return             返回符合条件的用户 ID 列表。
     */
    private List<Long> getListWithCursor(String key,
                                         int limit,
                                         Long cursor,
                                         IntFunction<Map<Long, Map<String, Object>>> rowsFetcher,
                                         String idField,
                                         String tsField) {
        // 计算最大分数：如果游标为空，则使用正无穷大作为上限；否则使用游标的值。
        double max = cursor == null ? Double.POSITIVE_INFINITY : cursor.doubleValue();
        // 从 Redis ZSet 中按分数范围倒序读取数据，范围是 (负无穷, max]，最多返回 limit 条记录。
        Set<String> cached = redis.opsForZSet().reverseRangeByScore(key, max, Double.NEGATIVE_INFINITY, 0, limit);
        // 如果缓存中有数据，直接转换为长整型列表并返回。
        if (cached != null && !cached.isEmpty()) {
            return toLongList(cached);
        }
        // 如果缓存未命中，计算需要从数据库中获取的记录数：
        // 至少获取 limit 条记录，但如果 limit 较小，则默认获取 100 条以提高缓存利用率。
        int need = Math.max(limit, 100);
        // 调用 rowsFetcher 从数据库中获取原始行数据，最多获取 1000 条以防数据量过大。
        Map<Long, Map<String, Object>> rows = rowsFetcher.apply(Math.min(need, 1000));
        // 如果数据库中有数据：
        if (rows != null && !rows.isEmpty()) {
            // 将数据库中的行数据填充到 Redis ZSet 中，同时过滤掉高于游标分数的记录。
            fillZSet(key, rows, idField, tsField, cursor);
            // 设置 ZSet 的过期时间为 2 小时，避免长期占用内存。
            redis.expire(key, Duration.ofHours(2));
            // 再次从 Redis ZSet 中按分数范围倒序读取数据。
            Set<String> filled = redis.opsForZSet().reverseRangeByScore(key, max, Double.NEGATIVE_INFINITY, 0, limit);
            // 如果读取到数据，转换为长整型列表并返回；否则返回空列表。
            return filled == null ? Collections.emptyList() : toLongList(filled);
        }
        // 如果数据库也没有数据，返回空列表。
        return Collections.emptyList();
    }

    /**
     * 将行数据填充至 ZSet：分值为创建时间戳；若提供游标则只填充不高于游标的记录。
     */
    private void fillZSet(String key,
                          Map<Long, Map<String, Object>> rows,
                          String idField,
                          String tsField,
                          Long cursor) {
        for (Map<String, Object> r : rows.values()) {
            Object idObj = r.get(idField);
            Object tsObj = r.get(tsField);
            if (idObj == null || tsObj == null) continue;
            //将多类型时间对象统一转换为毫秒分值。
            long score = tsScore(tsObj);
            if (cursor == null || score <= cursor) {
                redis.opsForZSet().add(key, String.valueOf(idObj), score);
            }
        }
    }

    /**
     * 将多类型时间对象统一转换为毫秒分值。
     */
    private long tsScore(Object tsObj) {
        if (tsObj instanceof Timestamp ts) {
            return ts.getTime();
        }
        if (tsObj instanceof Date d) {
            return d.getTime();
        }
        return System.currentTimeMillis();
    }

    /**
     * 将字符串集合按原顺序映射为长整型列表。
     */
    private List<Long> toLongList(Set<String> set) {
        List<Long> out = new ArrayList<>(set.size());
        for (String s : set) out.add(Long.valueOf(s));
        return out;
    }

    /**
     * 更新本地 Top 缓存：大V 用户仅缓存前 500 名，减少频繁回源与排序成本。
     */
    private void maybeUpdateTopCache(long userId, String key, Cache<Long, List<Long>> cache) {
        Set<String> allSet = redis.opsForZSet().reverseRange(key, 0, 499);
        if (allSet == null || allSet.isEmpty()) return;
        List<Long> all = new ArrayList<>(allSet.size());
        for (String s : allSet) all.add(Long.valueOf(s));
        cache.put(userId, all);
    }


    //令牌桶（Token Bucket）  流量控制算法、通过令牌控制访问速度
    private static final String TOKEN_BUCKET_LUA = """         
            local key = KEYS[1]
            local capacity = tonumber(ARGV[1])
            local rate = tonumber(ARGV[2])
            local now = redis.call('TIME')[1]
            local last = redis.call('HGET', key, 'last')
            local tokens = redis.call('HGET', key, 'tokens')
            if not last then last = now; tokens = capacity end
            local elapsed = tonumber(now) - tonumber(last)
            local add = elapsed * rate
            tokens = math.min(capacity, tonumber(tokens) + add)
            if tokens < 1 then redis.call('HSET', key, 'last', now); redis.call('HSET', key, 'tokens', tokens); return 0 end
            tokens = tokens - 1
            redis.call('HSET', key, 'last', now)
            redis.call('HSET', key, 'tokens', tokens)
            redis.call('PEXPIRE', key, 60000)
            return 1
            """;



}
