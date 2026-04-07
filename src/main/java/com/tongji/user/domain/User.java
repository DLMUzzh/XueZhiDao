package com.tongji.user.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDate;

/**
 * 用户实体类
 * 使用了Lombok注解自动生成getter、setter、toString等方法
 * 支持通过Builder模式构建对象，以及无参和全参构造方法
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class User {
    // 用户唯一标识符
    private Long id;
    // 用户手机号码
    private String phone;
    // 用户电子邮箱
    private String email;
    // 密码的哈希值，用于安全存储
    private String passwordHash;
    // 用户昵称
    private String nickname;
    // 用户头像URL
    private String avatar;
    // 用户个人简介
    private String bio;
    // 用户在的ID
    private String zgId;
    // 用户性别
    private String gender;
    // 用户生日，使用LocalDate类型确保日期格式正确
    private LocalDate birthday;
    // 用户所在学校
    private String school;
    // 用户标签的JSON格式字符串，用于存储多个标签信息
    private String tagsJson;
    // 用户创建时间，使用Instant类型精确记录时间点
    private Instant createdAt;
    // 用户信息最后更新时间，使用Instant类型精确记录时间点
    private Instant updatedAt;
}

