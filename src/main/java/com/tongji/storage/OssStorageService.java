package com.tongji.storage;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.tongji.storage.config.OssProperties;
import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.Instant;
import java.net.URL;
import java.util.Date;

@Service
@RequiredArgsConstructor
public class OssStorageService {

    private final OssProperties props;


    /**
     * 上传用户头像到OSS存储
     * <p>
     * 该方法负责将用户上传的头像文件存储到阿里云OSS，并返回可访问的URL
     * </p>
     *
     * @param userId 用户ID，用于生成唯一的文件路径
     * @param file 上传的头像文件（MultipartFile格式）
     * @return 上传完成后文件的公开访问URL
     * @throws BusinessException 当OSS配置缺失或文件读取失败时抛出
     */
    public String uploadAvatar(long userId, MultipartFile file) {
        // 确保OSS配置完整可用
        ensureConfigured();
        // 获取原始文件名
        String original = file.getOriginalFilename();
        String ext = "";
        // 提取文件扩展名（如果有）
        if (original != null && original.contains(".")) {
            ext = original.substring(original.lastIndexOf('.'));
        }
        // 生成唯一对象键：目录/用户ID-时间戳.扩展名
        String objectKey = props.getFolder() + "/" + userId + "-" + Instant.now().toEpochMilli() + ext;
        // 创建OSS客户端实例
        OSS client = new OSSClientBuilder().build(props.getEndpoint(), props.getAccessKeyId(), props.getAccessKeySecret());
        try {
            // 创建上传请求，将文件输入流传给OSS
            PutObjectRequest request = new PutObjectRequest(props.getBucket(), objectKey, file.getInputStream());
            // 执行文件上传操作
            client.putObject(request);
        } catch (IOException e) {
            // 文件读取失败时抛出业务异常
            throw new BusinessException(ErrorCode.BAD_REQUEST, "头像文件读取失败");
        } finally {
            // 关闭OSS客户端释放资源
            client.shutdown();
        }
        // 返回文件的公共访问URL
        return publicUrl(objectKey);
    }



    /**
     * 生成OSS对象的公共访问URL
     * <p>
     * 根据配置情况选择不同的URL生成策略：
     * 1. 如果配置了自定义公共域名，则使用该域名构建URL
     * 2. 如果未配置自定义域名，则使用OSS默认的访问域名
     * </p>
     *
     * @param objectKey OSS对象的存储键（包括路径）
     * @return 可公开访问的对象URL
     */
    private String publicUrl(String objectKey) {
        // 检查是否存在自定义公共域名配置
        if (props.getPublicDomain() != null && !props.getPublicDomain().isBlank()) {
            // 如果配置了自定义公共域名，则使用该域名构建URL
            // replaceAll("/$", "") 用于移除域名末尾的斜杠（如果存在），避免产生双斜杠
            return props.getPublicDomain().replaceAll("/$", "") + "/" + objectKey;
        }
        // 如果没有配置自定义域名，则使用OSS默认的访问格式
        // 格式：https://bucket.endpoint/objectKey
        return "https://" + props.getBucket() + "." + props.getEndpoint() + "/" + objectKey;
    }

    /**
     * 生成用于直传的 PUT 预签名 URL。
     * 客户端必须在上传时设置与签名一致的 Content-Type。
     * @param objectKey 目标对象键
     * @param contentType 上传内容类型（如 text/markdown, image/png）
     * @param expiresInSeconds 有效期秒数（建议 300-900）
     * @return 可直接用于 PUT 上传的预签名 URL
     */
    public String generatePresignedPutUrl(String objectKey, String contentType, int expiresInSeconds) {
        // 验证OSS配置是否完整可用
        ensureConfigured();
        // 使用配置信息创建OSS客户端实例
        OSS client = new OSSClientBuilder().build(props.getEndpoint(), props.getAccessKeyId(), props.getAccessKeySecret());
        try {
            // 计算过期时间点：当前时间 + 有效秒数
            Date expiration = new Date(System.currentTimeMillis() + expiresInSeconds * 1000L);
            // 创建预签名URL请求对象，指定桶、对象键和HTTP方法为PUT
            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(props.getBucket(), objectKey, HttpMethod.PUT);
            // 设置URL过期时间
            request.setExpiration(expiration);
            // 如果指定了内容类型，则设置到请求中，这样上传时必须使用相同的Content-Type
            if (contentType != null && !contentType.isBlank()) {
                request.setContentType(contentType);
            }
            // 生成预签名URL
            URL url = client.generatePresignedUrl(request);
            // 将URL转换为字符串格式返回
            return url.toString();
        } finally {
            // 无论成功与否，都要关闭OSS客户端释放连接资源
            client.shutdown();
        }
    }

    /**
     * 确保OSS配置可用
    **/
    private void ensureConfigured() {
        if (props.getEndpoint() == null || props.getAccessKeyId() == null || props.getAccessKeySecret() == null || props.getBucket() == null) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "对象存储未配置");
        }
    }
}
