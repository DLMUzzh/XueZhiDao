package com.tongji.user.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

import com.tongji.user.service.UserService;
import com.tongji.user.mapper.UserMapper;
import com.tongji.user.domain.User;

@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService {

    private final UserMapper userMapper;

    /**
     * 根据手机号查询用户。
     *
     * @param phone 手机号。
     * @return 用户 Optional。
     */
    @Transactional(readOnly = true)
    public Optional<User> findByPhone(String phone) {
        return Optional.ofNullable(userMapper.findByPhone(phone));
    }

    /**
     * 根据邮箱查询用户。
     *
     * @param email 邮箱地址。
     * @return 用户 Optional。
     */
    @Transactional(readOnly = true)
    public Optional<User> findByEmail(String email) {
        return Optional.ofNullable(userMapper.findByEmail(email));
    }

    /**
     * 根据 ID 查询用户。
     *
     * @param id 用户 ID。
     * @return 用户 Optional。
     */
    @Transactional(readOnly = true)
    public Optional<User> findById(long id) {
        return Optional.ofNullable(userMapper.findById(id));
    }

    /**
     * 判断手机号是否存在。
     *
     * @param phone 手机号。
     * @return 是否存在。
     */
    @Transactional(readOnly = true)
    public boolean existsByPhone(String phone) {
        return userMapper.existsByPhone(phone);
    }

    /**
     * 判断邮箱是否存在。
     *
     * @param email 邮箱地址。
     * @return 是否存在。
     */
    @Transactional(readOnly = true)
    public boolean existsByEmail(String email) {
        return userMapper.existsByEmail(email);
    }

    /**
     * 创建用户，写入创建与更新时间并持久化。
     * 该方法使用@Transactional注解确保事务性，保证数据一致性。
 * 在创建用户时，会自动设置用户的创建时间和更新时间为当前时间。
 *
     * @param user 待创建的用户实体。
     * @return 持久化后的用户实体。
     */
    @Transactional
    public User createUser(User user) {
    // 获取当前时间戳
        Instant now = Instant.now();
    // 设置用户的创建时间为当前时间
        user.setCreatedAt(now);
    // 设置用户的更新时间为当前时间
        user.setUpdatedAt(now);
    // 将用户信息插入到数据库中
        userMapper.insert(user);
    // 返回持久化后的用户实体
        return user;
    }

    /**
     * 更新用户密码哈希并写入更新时间。
     *
     * @param user 用户实体（需包含 ID 与新的 passwordHash）。
     */
    @Transactional
    public void updatePassword(User user) {
        user.setUpdatedAt(Instant.now());
        userMapper.updatePassword(user.getId(), user.getPasswordHash());
    }
}
