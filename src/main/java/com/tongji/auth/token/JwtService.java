package com.tongji.auth.token;

import lombok.RequiredArgsConstructor;
import com.tongji.auth.config.AuthProperties;
import com.tongji.user.domain.User;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/**
 * JWT 令牌服务。
 * <p>
 * 功能：签发 Access/Refresh Token（RS256），解码 JWT，提取用户 ID、令牌类型与令牌 ID。
 * 声明：
 * - `token_type`：标识 access 或 refresh；
 * - `uid`：用户ID；
 * - `jti`：令牌ID（用作 Refresh Token 的白名单键）。
 * 过期时间：来自 `AuthProperties.jwt.accessTokenTtl` 与 `refreshTokenTtl`。
 */
@Service
@RequiredArgsConstructor
public class JwtService {

    private static final String CLAIM_TOKEN_TYPE = "token_type";//令牌类型声明键
    private static final String CLAIM_USER_ID = "uid";//用户ID声明键

    private final JwtEncoder jwtEncoder; // JWT 编码器
    private final JwtDecoder jwtDecoder; // JWT 解码器
    private final AuthProperties properties; // 认证配置
    private final Clock clock = Clock.systemUTC(); // 时钟

    /**
     * 为指定用户签发一对 Access/Refresh Token。
     * <p>
     * 令牌类型通过 `token_type` 声明区分；Refresh Token 的 `jti` 用于白名单存储与撤销。
     * 过期时间取自配置 `AuthProperties.jwt`。
     *
     * @param user 用户实体。
     * @return 令牌对与对应过期时间及刷新令牌 ID。
     */
    public TokenPair issueTokenPair(User user) {
    // 生成唯一的刷新令牌ID
        String refreshTokenId = UUID.randomUUID().toString();
    // 获取当前时间作为签发时间
        Instant issuedAt = Instant.now(clock);
    // 计算访问令牌和刷新令牌的过期时间
        Instant accessExpiresAt = issuedAt.plus(properties.getJwt().getAccessTokenTtl());
        Instant refreshExpiresAt = issuedAt.plus(properties.getJwt().getRefreshTokenTtl());

    // 编码访问令牌，使用"access"作为令牌类型，并生成一个随机的jti
        String accessToken = encodeToken(user, issuedAt, accessExpiresAt, "access", UUID.randomUUID().toString());
    // 编码刷新令牌，使用之前生成的refreshTokenId作为jti
        String refreshToken = encodeRefreshToken(user, issuedAt, refreshExpiresAt, refreshTokenId);
    // 返回包含两个令牌及其过期时间和刷新令牌ID的对象
        return new TokenPair(accessToken, accessExpiresAt, refreshToken, refreshExpiresAt, refreshTokenId);
    }

    /**
     * 解码 JWT 字符串为 {@link Jwt}。
     *
     * @param token JWT 字符串。
     * @return 解析后的 JWT 对象。
     */
    public Jwt decode(String token) {
        return jwtDecoder.decode(token);
    }

    /**
     * 编码访问令牌。
     * 该方法用于生成一个JWT（JSON Web Token）令牌，包含了用户信息和令牌的基本属性。
 *
     * @param user      用户实体，作为 subject 与自定义声明来源。
     * @param issuedAt  签发时间。
     * @param expiresAt 过期时间。
     * @param tokenType 令牌类型（"access"）。
     * @param tokenId   令牌 ID（jti）。
     * @return 编码后的 JWT 字符串。
     */
    private String encodeToken(User user, Instant issuedAt, Instant expiresAt, String tokenType, String tokenId) {
        // 创建JWT声明集合，使用JwtClaimsSet构建器模式

        JwtClaimsSet claims = JwtClaimsSet.builder()// 以下声明都是Payload的内容
            // 设置颁发者，从配置属性中获取
                .issuer(properties.getJwt().getIssuer())
            // 设置签发时间
                .issuedAt(issuedAt)
            // 设置过期时间
                .expiresAt(expiresAt)
            // 设置主题（subject），使用用户ID
                .subject(String.valueOf(user.getId()))
            // 设置令牌唯一标识符
                .id(tokenId)
            // 添加自定义声明：令牌类型
                .claim(CLAIM_TOKEN_TYPE, tokenType)
            // 添加自定义声明：用户ID
                .claim(CLAIM_USER_ID, user.getId())
            // 添加自定义声明：用户昵称
                .claim("nickname", user.getNickname())
            // 构建完成，返回JwtClaimsSet对象
                .build();
    // 使用JWT编码器将声明集合编码为JWT字符串并返回
        return jwtEncoder.encode(JwtEncoderParameters.from(claims)).getTokenValue();
    }

    /**
     * 编码刷新令牌。
     *
     * @param user      用户实体。
     * @param issuedAt  签发时间。
     * @param expiresAt 过期时间。
     * @param tokenId   刷新令牌 ID（jti）。
     * @return 编码后的刷新令牌字符串。
     */
    private String encodeRefreshToken(User user, Instant issuedAt, Instant expiresAt, String tokenId) {
        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer(properties.getJwt().getIssuer())
                .issuedAt(issuedAt)
                .expiresAt(expiresAt)
                .subject(String.valueOf(user.getId()))
                .id(tokenId)
                .claim(CLAIM_TOKEN_TYPE, "refresh")
                .claim(CLAIM_USER_ID, user.getId())
                .build();
        return jwtEncoder.encode(JwtEncoderParameters.from(claims)).getTokenValue();
    }

    /**
     * 从 JWT 中提取用户 ID。
     * 该方法支持从数字或字符串类型的声明中提取用户 ID。
 *
     * @param jwt 已解析的 JWT。
     * @return 用户 ID（long）。
     * @throws IllegalArgumentException 当声明类型不合法时抛出。
     */
    public long extractUserId(Jwt jwt) {
    // 从 JWT 的声明中获取用户 ID 声明
        Object claim = jwt.getClaims().get(CLAIM_USER_ID);
    // 检查声明是否为数字类型，如果是则转换为 long 值返回
        if (claim instanceof Number number) {
            return number.longValue();
        }
    // 检查声明是否为字符串类型，如果是则解析为 long 值返回
        if (claim instanceof String text) {
            return Long.parseLong(text);
        }
    // 如果声明既不是数字也不是字符串，抛出非法参数异常
        throw new IllegalArgumentException("Invalid user id in token");
    }

    /**
     * 提取令牌类型声明。
     *
     * @param jwt 已解析的 JWT。
     * @return 令牌类型字符串（例如："access" 或 "refresh"）。
     */
    public String extractTokenType(Jwt jwt) {
        Object claim = jwt.getClaims().get(CLAIM_TOKEN_TYPE);
        return claim != null ? claim.toString() : "";
    }

    /**
     * 提取令牌 ID（jti）。
     *
     * @param jwt 已解析的 JWT。
     * @return 令牌 ID。
     */
    public String extractTokenId(Jwt jwt) {
        return jwt.getId();
    }
}
