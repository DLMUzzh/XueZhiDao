package com.tongji.auth.token;

import java.time.Instant;

/**
 * 访问令牌与刷新令牌的组合。
 * <p>
 * 字段说明：
 * - accessToken：访问令牌（JWT 字符串，Bearer 使用）；
 * - accessTokenExpiresAt：访问令牌过期时间；
 * - refreshToken：刷新令牌（JWT 字符串，仅用于刷新接口）；
 * - refreshTokenExpiresAt：刷新令牌过期时间；
 * - refreshTokenId：刷新令牌 ID（jti，用于白名单存储与撤销）。
 */
public record TokenPair(  // 定义一个名为TokenPair的记录类型，用于存储访问令牌和刷新令牌的组合
        String accessToken,  // 访问令牌，类型为String，表示JWT字符串，用于Bearer认证
        Instant accessTokenExpiresAt,  // 访问令牌的过期时间，类型为Instant，精确到纳秒
        String refreshToken,
        Instant refreshTokenExpiresAt,
        String refreshTokenId
) {
}
