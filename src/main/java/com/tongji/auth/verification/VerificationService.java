package com.tongji.auth.verification;

import lombok.RequiredArgsConstructor;
import com.tongji.auth.config.AuthProperties;
import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * 验证码业务服务。
 * <p>
 * 负责发送与校验验证码：
 * - 速率限制与日限额；
 * - 随机码生成与存储；
 * - 调用发送器进行实际发送；
 * 配置来源于 `AuthProperties.Verification`。
 */
@Service
@RequiredArgsConstructor
public class VerificationService {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final DateTimeFormatter DAY_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final VerificationCodeStore codeStore;
    private final CodeSender codeSender;
    private final StringRedisTemplate stringRedisTemplate;
    private final AuthProperties properties;


    /**
     * 发送验证码到指定标识。
     * <p>
     * 执行发送间隔与日次数限制，生成随机数字验证码，保存到存储并调用发送器。
     *
     * @param scene      验证码场景（REGISTER/LOGIN/RESET_PASSWORD）。
     * @param identifier 标识（手机号或邮箱）。
     * @return 发送结果，包含标识、场景与过期秒数。
     * @throws BusinessException 参数不完整或触发速率/日限额时抛出。
     */
    public SendCodeResult sendCode(VerificationScene scene, String identifier) {
        // 1. 参数校验：检查场景和标识是否为空
        if (scene == null || !StringUtils.hasText(identifier)) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "请提供正确的验证码发送参数");
        }

        // 2. 获取验证码配置信息（包括发送间隔、日限额、验证码长度、过期时间等）
        AuthProperties.Verification cfg = properties.getVerification();

        // 3. 执行发送频率限制检查：确保同一标识在规定间隔时间内只能发送一次验证码
        enforceSendInterval(scene, identifier, cfg.getSendInterval());

        // 4. 执行每日发送次数限制：确保每个标识每天发送验证码不超过设定上限
        enforceDailyLimit(scene, identifier, cfg.getDailyLimit());

        // 5. 生成指定长度的随机数字验证码
        String code = generateNumericCode(cfg.getCodeLength());
        System.out.println("生成的验证码是"+code);

        // 6. 将验证码保存到存储中（设置过期时间和最大尝试次数）  用于验证比对
        codeStore.saveCode(scene.name(), identifier, code, cfg.getTtl(), cfg.getMaxAttempts());

        // 7. 调用具体的发送器将验证码发送给用户（通过短信或邮件）
        codeSender.sendCode(scene, identifier, code, (int) cfg.getTtl().toMinutes());

        // 8. 返回发送结果，包含目标标识、验证场景和过期秒数
        return new SendCodeResult(identifier, scene, (int) cfg.getTtl().toSeconds());
    }


    /**
     * 校验验证码是否正确且未超限。
     *
     * @param scene      验证码场景。
     * @param identifier 标识（手机号或邮箱）。
     * @param code       用户输入的验证码。
     * @return 校验结果，包含状态与尝试次数统计。
     * @throws BusinessException 参数不完整时抛出。
     */
    public VerificationCheckResult verify(VerificationScene scene, String identifier, String code) {
        if (scene == null || !StringUtils.hasText(identifier) || !StringUtils.hasText(code)) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "验证码校验参数不完整");
        }
        return codeStore.verify(scene.name(), identifier, code);
    }

    /**
     * 使验证码失效（删除存储记录）。
     *
     * @param scene      验证码场景。
     * @param identifier 标识（手机号或邮箱）。
     */
    public void invalidate(VerificationScene scene, String identifier) {
        codeStore.invalidate(scene.name(), identifier);
    }


    /**
     * 发送间隔限制：同一标识在指定间隔内只能发送一次。
     *
     * @param scene      验证码场景。
     * @param identifier 标识（手机号或邮箱）。
     * @param interval   发送间隔。
     */
    private void enforceSendInterval(VerificationScene scene, String identifier, Duration interval) {
        // 1. 如果间隔时间为0或负数，表示不限制发送频率，直接返回
        if (interval.isZero() || interval.isNegative()) {
            return;
        }

        // 2. 构建Redis键值，用于标识该场景下该用户的最后发送时间记录
        String key = "auth:code:last:" + scene.name() + ":" + identifier;

        // 3. 查询Redis中是否存在该用户的最近发送记录
        String existing = stringRedisTemplate.opsForValue().get(key);

        // 4. 如果已存在发送记录，说明在间隔时间内再次请求发送，抛出频率限制异常
        if (existing != null) {
            throw new BusinessException(ErrorCode.VERIFICATION_RATE_LIMIT);
        }

        // 5. 如果没有发送记录，将当前发送标记存入Redis，并设置过期时间为间隔时间
        // 这样在间隔时间内再次请求时会被拦截，实现发送频率控制
        stringRedisTemplate.opsForValue().set(key, "1", interval);
    }


    /**
     * 每日发送次数限制：超过上限则抛出限额异常。
     *
     * @param scene      验证码场景。
     * @param identifier 标识（手机号或邮箱）。
     * @param limit      每日上限次数。
     */
    private void enforceDailyLimit(VerificationScene scene, String identifier, int limit) {


        // 1. 如果日限额小于等于0，表示不限制每日发送次数，直接返回
        if (limit <= 0) {
            return;
        }
        // 2. 获取当前日期字符串，格式为"yyyyMMdd"，用于构建按天统计的Redis键
        String date = DAY_FORMAT.format(LocalDate.now());
        // 3. 构建Redis键值，用于统计该用户在当天的验证码发送次数
        String key = "auth:code:count:" + scene.name() + ":" + identifier + ":" + date;

        // 4. 使用Redis的原子递增操作增加发送次数计数
        Long count = stringRedisTemplate.opsForValue().increment(key);

        // 5. 如果是当天第一次发送（计数值为1），设置该计数键的过期时间为1天，使其在第二天自动清除
        if (count != null && count == 1L) {
            stringRedisTemplate.expire(key, Duration.ofDays(1));
        }
        // 6. 检查当前发送次数是否超过日限额，如果超过则抛出日限额限制异常
        if (count != null && count > limit) {
            throw new BusinessException(ErrorCode.VERIFICATION_DAILY_LIMIT);
        }
    }

    /**
     * 生成指定长度的纯数字验证码。
     *
     * @param length 验证码长度。
     * @return 数字字符串。
     */
    private static String generateNumericCode(int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(RANDOM.nextInt(10));
        }
        return builder.toString();
    }
}
