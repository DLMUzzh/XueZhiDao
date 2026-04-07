package com.tongji.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

@Configuration
public class RedissonConfig {
    @Value("${counter.rebuild.lock.watchdog-ms:30000}")
    private long lockWatchdogMs;

/**
 * 创建并配置 Redisson 客户端 Bean
 * @param redisProperties Redis 配置属性，包含主机、端口、密码等信息
 * @return 配置完成的 RedissonClient 实例
 */
    @Bean
    public RedissonClient redissonClient(RedisProperties redisProperties) {
    // 创建 Redisson 配置对象
        Config config = new Config();
        // 配置 Redisson 的锁看门狗超时，用于自动续约锁
        config.setLockWatchdogTimeout(lockWatchdogMs);
    // 构建 Redis 连接地址
        String address = "redis://" + redisProperties.getHost() + ":" + redisProperties.getPort();
    // 配置单机 Redis 服务器连接
        SingleServerConfig single = config.useSingleServer().setAddress(address);

    // 如果配置了 Redis 密码，则设置密码
        if (redisProperties.getPassword() != null && !redisProperties.getPassword().isEmpty()) {
            single.setPassword(redisProperties.getPassword());
        }

    // 设置 Redis 数据库索引
        // Spring Boot RedisProperties#getDatabase 返回的是原始 int（默认 0），无需判空
        single.setDatabase(redisProperties.getDatabase());
    // 创建并返回 RedissonClient 实例
        return Redisson.create(config);
    }
}