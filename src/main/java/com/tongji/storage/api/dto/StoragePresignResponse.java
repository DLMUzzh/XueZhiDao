package com.tongji.storage.api.dto;

import java.util.Map;

/**
 * 预签名直传响应。
 */
public record StoragePresignResponse(
        String objectKey, // 存储对象键
        String putUrl, // 预签名URL
        Map<String, String> headers, // 预签名URL的请求头
        int expiresIn   //预签名URL过期时间
) {}