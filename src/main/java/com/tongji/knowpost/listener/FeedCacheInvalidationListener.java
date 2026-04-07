package com.tongji.knowpost.listener;

import com.tongji.counter.event.CounterEvent;
import com.tongji.knowpost.service.FeedCacheService;
import com.github.benmanes.caffeine.cache.Cache;
import com.tongji.knowpost.api.dto.FeedPageResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Feed缓存失效监听器组件，用于处理计数器变化事件并更新相关缓存
 */
@Component
public class FeedCacheInvalidationListener {


    private final FeedCacheService feedCacheService;
    private final Cache<String, FeedPageResponse> feedPublicCache;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final com.tongji.counter.service.UserCounterService userCounterService;
    private final com.tongji.knowpost.mapper.KnowPostMapper knowPostMapper;

    /**
     * 构造函数，注入所需的服务
     *
     * @param feedCacheService Feed缓存服务
     * @param feedPublicCache 公共Feed缓存
     * @param redis Redis模板
     * @param objectMapper 对象映射器
     * @param userCounterService 用户计数器服务
     * @param knowPostMapper 知晓帖子映射器
     */
    public FeedCacheInvalidationListener(FeedCacheService feedCacheService,
                                         @Qualifier("feedPublicCache") Cache<String, FeedPageResponse> feedPublicCache,
                                         StringRedisTemplate redis,
                                         ObjectMapper objectMapper,
                                         com.tongji.counter.service.UserCounterService userCounterService,
                                         com.tongji.knowpost.mapper.KnowPostMapper knowPostMapper) {
        this.feedCacheService = feedCacheService;
        this.feedPublicCache = feedPublicCache;
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.userCounterService = userCounterService;
        this.knowPostMapper = knowPostMapper;
    }

/**
 * 监听计数器变化事件的处理方法
 * 当计数器发生变化时，根据事件类型和指标执行相应的处理逻辑
 *
 * @param event 计数器事件对象，包含事件类型、实体ID、指标类型和变化量等信息
 */
    @EventListener
    public void onCounterChanged(CounterEvent event) {
    // 检查事件类型是否为"knowpost"，如果不是则直接返回
        if (!"knowpost".equals(event.getEntityType())) return;
    // 获取事件指标类型
        String metric = event.getMetric();
    // 处理点赞或收藏事件
        if ("like".equals(metric) || "fav".equals(metric)) {
        // 获取实体ID和变化量
            String eid = event.getEntityId();
            int delta = event.getDelta();
            try {
            // 根据实体ID查找对应的帖子信息
                com.tongji.knowpost.model.KnowPost post = knowPostMapper.findById(Long.valueOf(eid));
            // 检查帖子是否存在且创建者ID不为空
                if (post != null && post.getCreatorId() != null) {
                // 获取帖子创建者ID
                    long owner = post.getCreatorId();
                // 根据指标类型更新用户对应的计数器
                    if ("like".equals(metric)) userCounterService.incrementLikesReceived(owner, delta);
                    if ("fav".equals(metric)) userCounterService.incrementFavsReceived(owner, delta);
                }
            } catch (Exception ignored) {} // 捕获并忽略所有异常
        // 更新计数缓存
            updateCountCache(eid, metric, delta);
        // 计算当前小时的时间戳
            long hourSlot = System.currentTimeMillis() / 3600000L;
        // 创建键集合
            java.util.Set<String> keys = new java.util.LinkedHashSet<>();
        // 获取当前小时对应的键集合
            java.util.Set<String> cur = redis.opsForSet().members("feed:public:index:" + eid + ":" + hourSlot);
            if (cur != null) keys.addAll(cur);
        // 获取上一小时对应的键集合
            java.util.Set<String> prev = redis.opsForSet().members("feed:public:index:" + eid + ":" + (hourSlot - 1));
            if (prev != null) keys.addAll(prev);
        // 如果键集合为空则直接返回
            if (keys == null || keys.isEmpty()) return;
        // 遍历所有键
            for (String key : keys) {
            // 获取本地缓存中的FeedPageResponse
                FeedPageResponse local = feedPublicCache.getIfPresent(key);
                if (local != null) {
                // 调整本地缓存中的页面计数
                    FeedPageResponse updatedLocal = adjustPageCounts(local, eid, metric, delta, true);
                    feedPublicCache.put(key, updatedLocal);
                }
            // 获取Redis中的缓存数据
                String cached = redis.opsForValue().get(key);
                if (cached != null) {
                    try {
                    // 将缓存数据转换为FeedPageResponse对象
                        FeedPageResponse resp = objectMapper.readValue(cached, FeedPageResponse.class);
                    // 调整页面计数并更新缓存
                        FeedPageResponse updated = adjustPageCounts(resp, eid, metric, delta, false);
                        writePageJsonKeepingTtl(key, updated);
                    } catch (Exception ignored) {} // 捕获并忽略所有异常
                } else {
                // 如果缓存不存在，从集合中移除对应的键
                    redis.opsForSet().remove("feed:public:index:" + eid + ":" + hourSlot, key);
                }
            }
        }
    }
    /**
     * 更新计数缓存
     *
     * @param eid 实体ID
     * @param metric 指标类型（点赞或收藏）
     * @param delta 变化量
     */
    private void updateCountCache(String eid, String metric, int delta) {
        String cntKey = "feed:count:" + eid;
        String cntJson = redis.opsForValue().get(cntKey);
        java.util.Map<String, Long> cm = null;
        if (cntJson != null) {
            try { cm = objectMapper.readValue(cntJson, new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Long>>(){}); } catch (Exception ignored) {}
        }
        if (cm == null) cm = new java.util.LinkedHashMap<>();
        Long like = cm.getOrDefault("like", 0L);
        Long fav = cm.getOrDefault("fav", 0L);
        if ("like".equals(metric)) like = Math.max(0L, like + delta);
        if ("fav".equals(metric)) fav = Math.max(0L, fav + delta);
        cm.put("like", like);
        cm.put("fav", fav);
        try {
            String j = objectMapper.writeValueAsString(cm);
            Long ttl = redis.getExpire(cntKey);
            if (ttl != null && ttl > 0) redis.opsForValue().set(cntKey, j, java.time.Duration.ofSeconds(ttl)); else redis.opsForValue().set(cntKey, j);
        } catch (Exception ignored) {}
    }

    /**
     * 调整页面计数
     *
     * @param page Feed页面响应对象
     * @param eid 实体ID
     * @param metric 指标类型（点赞或收藏）
     * @param delta 变化量
     * @param preserveUserFlags 是否保留用户标志
     * @return 调整后的Feed页面响应对象
     */
    private FeedPageResponse adjustPageCounts(FeedPageResponse page, String eid, String metric, int delta, boolean preserveUserFlags) {
        java.util.List<com.tongji.knowpost.api.dto.FeedItemResponse> items = new java.util.ArrayList<>(page.items().size());
        for (com.tongji.knowpost.api.dto.FeedItemResponse it : page.items()) {
                if (eid.equals(it.id())) {
                    Long like = it.likeCount();
                    Long fav = it.favoriteCount();
                    if ("like".equals(metric)) like = Math.max(0L, (like == null ? 0L : like) + delta);
                    if ("fav".equals(metric)) fav = Math.max(0L, (fav == null ? 0L : fav) + delta);
                    Boolean liked = preserveUserFlags ? it.liked() : null;
                    Boolean faved = preserveUserFlags ? it.faved() : null;
                    it = new com.tongji.knowpost.api.dto.FeedItemResponse(it.id(), it.title(), it.description(), it.coverImage(), it.tags(), it.authorAvatar(), it.authorNickname(), it.tagJson(), like, fav, liked, faved, it.isTop());
                }
                items.add(it);
            }
        return new FeedPageResponse(items, page.page(), page.size(), page.hasMore());
    }

    /**
     * 写入页面JSON并保持TTL
     *
     * @param key 缓存键
     * @param page Feed页面响应对象
     */
    private void writePageJsonKeepingTtl(String key, FeedPageResponse page) {
        try {
            String json = objectMapper.writeValueAsString(page);
            Long ttl = redis.getExpire(key);
            if (ttl != null && ttl > 0) {
                redis.opsForValue().set(key, json, java.time.Duration.ofSeconds(ttl));
            } else {
                redis.opsForValue().set(key, json);
            }
        } catch (Exception ignored) {}
    }
}
