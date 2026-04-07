package com.tongji.knowpost.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Feed缓存服务类
 * 提供了处理动态缓存的相关方法，包括删除公共动态缓存和用户个人动态缓存
 * 使用了双重删除策略以确保缓存一致性
 */
@Service
@RequiredArgsConstructor
public class FeedCacheService {

    private final StringRedisTemplate redis; // Redis操作模板

    /**
     * 删除所有公共动态缓存
     * 通过Redis的keys命令匹配并删除所有以"feed:public:"开头的键
     */
    public void deleteAllFeedCaches() {
        Set<String> keys = redis.keys("feed:public:*");
        if (keys != null && !keys.isEmpty()) {
            redis.delete(keys);
        }
    }

    /**
     * 双重删除所有公共动态缓存
     * 通过两次删除中间加入短暂延迟的方式，确保缓存一致性
     * @param delayMillis 两次删除之间的延迟时间（毫秒）
     */
    public void doubleDeleteAll(long delayMillis) {
        deleteAllFeedCaches();
        try {
            Thread.sleep(Math.max(delayMillis, 50)); // 确保至少延迟50毫秒
        } catch (InterruptedException ignored) {} // 忽略中断异常
        deleteAllFeedCaches();
    }

    public void deleteMyFeedCaches(long userId) {
        Set<String> keys = redis.keys("feed:mine:" + userId + ":*");
        if (keys != null && !keys.isEmpty()) {
            redis.delete(keys);
        }
    }

    public void doubleDeleteMy(long userId, long delayMillis) {
        deleteMyFeedCaches(userId);
        try {
            Thread.sleep(Math.max(delayMillis, 50));
        } catch (InterruptedException ignored) {}
        deleteMyFeedCaches(userId);
    }
}