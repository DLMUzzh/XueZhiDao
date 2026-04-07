package com.tongji.llm.rag;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.tongji.knowpost.mapper.KnowPostMapper;
import com.tongji.knowpost.model.KnowPostDetailRow;
import com.tongji.config.EsProperties;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import java.util.*;

/**
 * RAG 索引构建服务：
 * - 将公开且已发布的知文切片并写入向量库
 * - 通过指纹（SHA256/ETag）判断是否需要重建，保证幂等
 * - 采用 delete-by-query 清理旧切片，再批量 upsert 新切片
 */
@Service
@RequiredArgsConstructor
public class RagIndexService {
    private static final Logger log = LoggerFactory.getLogger(RagIndexService.class);
    // 向量库封装（Elasticsearch VectorStore），负责写入/检索向量
    private final VectorStore vectorStore;
    // 数据访问：根据 postId 查询知文详情（含 contentUrl、指纹等）
    private final KnowPostMapper knowPostMapper;
    // 拉取 Markdown 正文内容
    private final RestTemplate http = new RestTemplate();
    // 直接使用ES客户端做指纹判断和删除旧切片
    private final ElasticsearchClient es;
    // ES 相关配置（索引名等）
    private final EsProperties esProps;

    /**
     * 确保指定的知文（postId）已被索引。
     * 如果尚未索引或内容已更新，则触发重新索引流程。
     *
     * @param postId 知文唯一标识符
     */
    public void ensureIndexed(long postId) {
        // 当前策略：在问答前直接尝试重建（指纹未变化时会跳过）
        reindexSinglePost(postId);
    }

    /**
     * 对单个知文执行索引重建操作。
     * 包括指纹校验、内容拉取、文本切片、元数据组装和向量写入等步骤。
     *
     * @param postId 知文唯一标识符
     * @return 成功写入的切片数量；若失败或无变更则返回 0
     */
    public int reindexSinglePost(long postId) {
        // 查询知文详情信息
        KnowPostDetailRow row = knowPostMapper.findDetailById(postId);
        if (row == null) {
            log.warn("Post {} not found", postId);
            return 0;
        }

        // 仅对状态为 published 且可见性为 public 的知文进行索引
        if (!"published".equalsIgnoreCase(row.getStatus()) || !"public".equalsIgnoreCase(row.getVisible())) {
            log.warn("Post {} is not public/published, skip indexing", postId);
            return 0;
        }

        // 检查内容 URL 是否存在，缺失则无法继续处理
        if (!StringUtils.hasText(row.getContentUrl())) {
            log.warn("Post {} missing contentUrl or not found", postId);
            return 0;
        }

        // 获取当前内容的指纹（SHA256 和 ETag）
        String currentSha = row.getContentSha256();
        String currentEtag = row.getContentEtag();

        // 判断是否需要重建索引：指纹未变化则跳过
        if (isUpToDate(postId, currentSha, currentEtag)) {
            log.info("Post {} already indexed with same fingerprint, skip", postId);
            return 0;
        }

        // 拉取 Markdown 正文内容
        String text = fetchContent(row.getContentUrl());
        if (!StringUtils.hasText(text)) {
            log.warn("Post {} content empty", postId);
            return 0;
        }

        // 按 Markdown 标题分割段落，再进一步切片
        List<String> chunks = chunkMarkdown(text);

        // 删除旧的切片数据，确保幂等性
        deleteExistingChunks(postId);

        // 构建 Document 列表，包含文本内容和业务元数据
        List<Document> docs = new ArrayList<>(chunks.size());
        for (int i = 0; i < chunks.size(); i++) {
            String cid = postId + "#" + i; // 构造切片 ID
            Map<String, Object> meta = new HashMap<>();
            meta.put("postId", String.valueOf(postId));         // 知文 ID
            meta.put("chunkId", cid);                           // 切片 ID
            meta.put("position", i);                            // 切片顺序
            meta.put("contentEtag", currentEtag);              // 内容 ETag
            meta.put("contentSha256", currentSha);             // 内容 SHA256
            meta.put("contentUrl", row.getContentUrl());       // 内容 URL
            meta.put("title", row.getTitle());                 // 知文标题
            docs.add(new Document(chunks.get(i), meta));       // 组装 Document
        }

        try {
            // 批量写入向量库
            vectorStore.add(docs);
        } catch (Exception e) {
            log.error("VectorStore add failed: {}", e.getMessage());
            return 0;
        }

        // 返回成功写入的切片数量
        return docs.size();
    }

    /**
     * 检查指定知文是否已索引且内容未发生变化。
     * 通过比对 SHA256 或 ETag 来实现指纹校验。
     *
     * @param postId      知文唯一标识符
     * @param currentSha  当前内容的 SHA256 值
     * @param currentEtag 当前内容的 ETag 值
     * @return 若索引内容与当前内容一致则返回 true，否则返回 false
     */
    private boolean isUpToDate(long postId, String currentSha, String currentEtag) {
        try {
            // 若未配置索引名，则无法查询，视为需要重建
            if (!StringUtils.hasText(esProps.getIndex())) {
                return false;
            }

            // 查询任意一条匹配 postId 的索引文档
            SearchResponse<Map> resp = es.search(s -> s
                            .index(esProps.getIndex())
                            .size(1)
                            .query(q -> q.term(t -> t
                                    .field("metadata.postId")
                                    .value(v -> v.stringValue(String.valueOf(postId))))),
                    Map.class);

            List<Hit<Map>> hits = resp.hits().hits();
            if (hits == null || hits.isEmpty()) return false;

            Map source = hits.getFirst().source();
            if (source == null) return false;

            Object metaObj = source.get("metadata");
            if (!(metaObj instanceof Map<?, ?> meta)) return false;

            // 提取索引中的指纹信息
            String indexedSha = asString(meta.get("contentSha256"));
            String indexedEtag = asString(meta.get("contentEtag"));

            // 优先比较 SHA256，其次比较 ETag
            if (StringUtils.hasText(currentSha) && StringUtils.hasText(indexedSha)) {
                return Objects.equals(currentSha, indexedSha);
            }
            if (StringUtils.hasText(currentEtag) && StringUtils.hasText(indexedEtag)) {
                return Objects.equals(currentEtag, indexedEtag);
            }

            return false;
        } catch (Exception e) {
            log.warn("Fingerprint check failed for post {}: {}", postId, e.getMessage());
            return false;
        }
    }

    /**
     * 删除指定知文的所有已索引切片。
     * 通过 delete-by-query 实现精确删除，保障 upsert 操作的幂等性。
     *
     * @param postId 知文唯一标识符
     */
    private void deleteExistingChunks(long postId) {
        try {
            // 若未配置索引名，则跳过删除操作
            if (!StringUtils.hasText(esProps.getIndex())) return;

            // 执行 delete-by-query 删除所有匹配 postId 的文档
            es.deleteByQuery(d -> d
                    .index(esProps.getIndex())
                    .query(q -> q.term(t -> t
                            .field("metadata.postId")
                            .value(v -> v.stringValue(String.valueOf(postId))))));
        } catch (Exception e) {
            log.warn("Delete old chunks failed for post {}: {}", postId, e.getMessage());
        }
    }

    /**
     * 工具方法：安全地将对象转换为字符串。
     *
     * @param o 待转换的对象
     * @return 转换后的字符串；若对象为 null 则返回 null
     */
    private static String asString(Object o) {
        return o == null ? null : String.valueOf(o);
    }

    /**
     * 从指定 URL 拉取 Markdown 正文内容。
     *
     * @param url 内容地址
     * @return 拉取到的文本内容；若失败则返回 null
     */
    private String fetchContent(String url) {
        try {
            return http.getForObject(url, String.class);
        } catch (Exception e) {
            log.error("Fetch content failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 按 Markdown 标题将文本分割为段落。
     * 遇到新标题时结束上一段，开始新的一段。
     *
     * @param text 原始 Markdown 文本
     * @return 分割后的段落列表
     */
    private List<String> chunkMarkdown(String text) {
        List<String> paras = new ArrayList<>();  // 用于存储分割后的段落列表
        String[] lines = text.split("\r?\n"); // 按行拆分文本，支持Windows和Unix换行符
        StringBuilder buf = new StringBuilder(); // 用于构建当前段落的缓冲区
        // 遍历每一行文本
        for (String line : lines) {
            boolean isHeader = line.startsWith("#"); // 判断是否为标题行
            if (isHeader && !buf.isEmpty()) {        // 遇到新标题，收束上一段
                paras.add(buf.toString());
                buf.setLength(0);
            }
            buf.append(line).append('\n'); // 将当前行追加到缓冲区
        }

        if (!buf.isEmpty()) paras.add(buf.toString()); // 处理最后一段
        return getChunks(paras); // 进一步切片
    }

    /**
     * 将段落按固定长度（≤800 字符）切片，并保留 100 字符重叠。
     * 旨在兼顾检索召回率和上下文语义连续性。
     *
     * @param paras 段落列表
     * @return 切片后的文本块列表
     */
    private static List<String> getChunks(List<String> paras) {
        List<String> chunks = new ArrayList<>();
        for (String p : paras) {
            if (p.length() <= 800) {
                chunks.add(p); // 不超过阈值则直接加入
            } else {
                int start = 0;
                while (start < p.length()) {
                    int end = Math.min(start + 800, p.length()); // 确定切片终点
                    chunks.add(p.substring(start, end));         // 加入当前切片
                    if (end >= p.length()) break;
                    start = Math.max(end - 100, start + 1);      // 保留 100 字符重叠
                }
            }
        }
        return chunks;
    }
}