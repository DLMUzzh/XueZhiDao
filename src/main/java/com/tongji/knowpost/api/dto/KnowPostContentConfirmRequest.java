package com.tongji.knowpost.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * 内容上传确认请求。
 * 这是一个Java 14引入的Record类型，用于不可变的数据传输对象。
 * Record类型会自动生成equals()、hashCode()、toString()等方法以及所有字段的访问器。
 */
public record KnowPostContentConfirmRequest(
        // 对象键，用于标识上传的内容对象，不能为空
        @NotBlank String objectKey,
        // 实体标签(ETag)，用于验证内容是否被修改，不能为空
        @NotBlank String etag,
        // 内容大小，以字节为单位，不能为空
        @NotNull Long size,
        // 内容的SHA256哈希值，用于完整性校验，不能为空
        @NotBlank String sha256
) {}