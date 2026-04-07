package com.tongji.storage.api;

import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import com.tongji.auth.token.JwtService;
import com.tongji.knowpost.mapper.KnowPostMapper;
import com.tongji.knowpost.model.KnowPost;
import com.tongji.storage.OssStorageService;
import com.tongji.storage.api.dto.StoragePresignRequest;
import com.tongji.storage.api.dto.StoragePresignResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/storage")
@Validated
@RequiredArgsConstructor
public class StorageController {

    private final OssStorageService ossStorageService;
    private final JwtService jwtService;
    private final KnowPostMapper knowPostMapper;


    /**
     * 获取用于直传的 PUT 预签名 URL。
     * @param request 包含上传请求参数（postId、场景、文章id、内容类型）
     * @param jwt 当前用户身份的JWT令牌
     * @return 包含预签名URL及相关信息的响应对象
     */
    @PostMapping("/presign")
    public StoragePresignResponse presign(@Valid @RequestBody StoragePresignRequest request,
                                          @AuthenticationPrincipal Jwt jwt) {
        // 从JWT中提取当前用户ID
        long userId = jwtService.extractUserId(jwt);

        long postId;
        // 验证并解析postId参数
        try {
            postId = Long.parseLong(request.postId());
        } catch (NumberFormatException e) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "postId 非法");
        }

        // 根据postId查询KnowPost
        // 权限校验：postId必须属于当前用户
        KnowPost post = knowPostMapper.findById(postId);
        if (post == null || post.getCreatorId() == null || post.getCreatorId() != userId) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "草稿不存在或无权限");
        }

        // 获取上传场景（knowpost_content 或 knowpost_image）
        String scene = request.scene();
        String objectKey;
        // 根据请求参数规范化扩展名
        String ext = normalizeExt(request.ext(), request.contentType(), scene);

        // 根据不同场景生成不同的对象存储路径
        if ("knowpost_content".equals(scene)) {
            // 知识帖子内容的存储路径
            objectKey = "posts/" + postId + "/content" + ext;
        } else if ("knowpost_image".equals(scene)) {
            // 知识帖子图片的存储路径，包含日期和随机字符串
            String date = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.of("UTC")).format(Instant.now());
            String rand = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 8);
            objectKey = "posts/" + postId + "/images/" + date + "/" + rand + ext;
        } else {
            // 不支持的上传场景
            throw new BusinessException(ErrorCode.BAD_REQUEST, "不支持的上传场景");
        }

        // 设置预签名URL过期时间为10分钟
        int expiresIn = 600; // 10 分钟
        // 生成预签名的PUT上传URL
        String putUrl = ossStorageService.generatePresignedPutUrl(objectKey, request.contentType(), expiresIn);
        // 创建请求头，指定内容类型
        Map<String, String> headers = Map.of("Content-Type", request.contentType());
        // 返回预签名URL相关信息
        // http://zhicheng-zzh.oss-cn-beijing.aliyuncs.com/posts/287858999783723008/content.md?
        // Expires=1772698814
        // &OSSAccessKeyId=LTAI5t9JZnpkoXPtnqY96sSb
        // &Signature=umjcKPeAIcQW%2F7jBlnRaCo4xM6I%3D
        return new StoragePresignResponse(objectKey, putUrl, headers, expiresIn);
    }


    //根据scene场景和contentType内容类型，返回扩展名
    private String normalizeExt(String ext, String contentType, String scene) {
        if (ext != null && !ext.isBlank()) {
            return ext.startsWith(".") ? ext : "." + ext;
        }
        if ("knowpost_content".equals(scene)) {
            return switch (contentType) {
                case "text/markdown" -> ".md";
                case "text/html" -> ".html";
                case "text/plain" -> ".txt";
                case "application/json" -> ".json";
                default -> ".bin";
            };
        } else {
            return switch (contentType) {
                case "image/jpeg" -> ".jpg";
                case "image/png" -> ".png";
                case "image/webp" -> ".webp";
                default -> ".img";
            };
        }
    }
}
