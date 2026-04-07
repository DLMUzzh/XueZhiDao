package com.tongji.relation.processor;

import com.tongji.relation.event.RelationEvent;
import com.tongji.relation.mapper.RelationMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import com.tongji.counter.service.UserCounterService;
import org.springframework.stereotype.Service;
import java.time.Duration;

/**
 * 关系事件处理器。
 * 职责：对 FollowCreated/FollowCanceled 事件进行去重、防抖与幂等处理，落库更新粉丝表，
 * 维护关注/粉丝 ZSet 缓存与 TTL，并原子更新用户维度计数（SDS）。
 */
@Service
public class RelationEventProcessor {
    private final RelationMapper mapper; // 数据访问层，用于操作关系数据表
    private final StringRedisTemplate redis; // Redis 操作模板，用于缓存和去重
    private final UserCounterService userCounterService; // 用户计数服务，用于更新关注数和粉丝数

    /**
     * 构造函数注入依赖
     * @param mapper 关系映射器
     * @param redis Redis 模板
     * @param userCounterService 用户计数服务
     */
    public RelationEventProcessor(RelationMapper mapper, StringRedisTemplate redis, UserCounterService userCounterService) {
        this.mapper = mapper;
        this.redis = redis;
        this.userCounterService = userCounterService;
    }

    /**
     * 处理关系事件：入库、更新缓存、刷新计数，并进行幂等去重。
     * @param evt 关系事件对象，包含事件类型、用户ID等信息
     */
    public void process(RelationEvent evt) {
        // 构造去重键，用于防止重复处理同一事件 dedup:rel:FollowCanceled:2:1:0
        String dk = "dedup:rel:" + evt.type() + ":" + evt.fromUserId() + ":" + evt.toUserId() + ":" + (evt.id() == null ? "0" : String.valueOf(evt.id()));
        // 使用 Redis 的 setIfAbsent 方法实现幂等性控制，设置 10 分钟的过期时间
        Boolean first = redis.opsForValue().setIfAbsent(dk, "1", Duration.ofMinutes(10));
        // 如果不是首次处理（即去重键已存在），则直接返回，避免重复处理
        if (first == null || !first) {
            return;
        }
        // 根据事件类型处理不同的业务逻辑
        if ("FollowCreated".equals(evt.type())) {
            // 插入粉丝关系到数据库
            mapper.insertFollower(evt.id(), evt.toUserId(), evt.fromUserId(), 1);
            // 获取当前时间戳，作为 ZSet 的分数
            long now = System.currentTimeMillis();
            // 更新关注者的关注列表缓存（ZSet），按时间排序
            redis.opsForZSet().add("uf:flws:" + evt.fromUserId(), String.valueOf(evt.toUserId()), now);
            // 更新被关注者的粉丝列表缓存（ZSet），按时间排序
            redis.opsForZSet().add("uf:fans:" + evt.toUserId(), String.valueOf(evt.fromUserId()), now);
            // 设置缓存的过期时间，减少陈旧数据的影响
            redis.expire("uf:flws:" + evt.fromUserId(), Duration.ofHours(2));
            redis.expire("uf:fans:" + evt.toUserId(), Duration.ofHours(2));
            // 原子性地增加用户的关注数和粉丝数
            userCounterService.incrementFollowings(evt.fromUserId(), 1);
            userCounterService.incrementFollowers(evt.toUserId(), 1);
        } else if ("FollowCanceled".equals(evt.type())) {
            // 删除粉丝关系
            mapper.cancelFollower(evt.toUserId(), evt.fromUserId());
            // 从关注者的关注列表缓存中移除被取消关注的用户
            redis.opsForZSet().remove("uf:flws:" + evt.fromUserId(), String.valueOf(evt.toUserId()));
            // 从被取消关注者的粉丝列表缓存中移除取消关注的用户
            redis.opsForZSet().remove("uf:fans:" + evt.toUserId(), String.valueOf(evt.fromUserId()));
            // 刷新缓存的过期时间
            redis.expire("uf:flws:" + evt.fromUserId(), Duration.ofHours(2));
            redis.expire("uf:fans:" + evt.toUserId(), Duration.ofHours(2));
            // 原子性地减少用户的关注数和粉丝数
            userCounterService.incrementFollowings(evt.fromUserId(), -1);
            userCounterService.incrementFollowers(evt.toUserId(), -1);
        }
    }
}
