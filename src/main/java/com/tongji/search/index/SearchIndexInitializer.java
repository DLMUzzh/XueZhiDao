package com.tongji.search.index;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.CompletionProperty;
import co.elastic.clients.elasticsearch._types.mapping.DateProperty;
import co.elastic.clients.elasticsearch._types.mapping.IntegerNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.KeywordProperty;
import co.elastic.clients.elasticsearch._types.mapping.LongNumberProperty;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;

/**
 * 搜索索引初始化：应用启动时确保索引与 Mapping 存在。
 * 注意：title/body 使用 IK 分词器，需在 ES 集群安装 analysis-ik 插件。
 */
@Service
@RequiredArgsConstructor
public class SearchIndexInitializer {
    private final ElasticsearchClient es;
    private static final String INDEX = "zhiguang_content_index";

/**
 * 使用@PostConstruct注解确保在Bean初始化后执行索引检查和创建
 * 该方法用于确保Elasticsearch索引存在，如果不存在则创建索引并设置映射
 */
    @PostConstruct
    public void ensureIndex() {
        try {
        // 检查索引是否已存在
            boolean exists = es.indices().exists(e -> e.index(INDEX)).value();
            if (exists) {
                return; // 如果索引已存在，直接返回
            }

        // 创建索引并设置映射
            es.indices().create(c -> c.index(INDEX).mappings(m -> m
                // content_id字段：长整型
                    .properties("content_id", Property.of(p -> p.long_(LongNumberProperty.of(b -> b))))
                // content_type字段：关键字类型
                    .properties("content_type", Property.of(p -> p.keyword(KeywordProperty.of(b -> b))))
                // description字段：文本类型，使用ik_max_word分词器
                    .properties("description", Property.of(p -> p.text(TextProperty.of(b -> b.analyzer("ik_max_word")))))
                    // IK 分词：title 使用 ik_max_word，检索使用 ik_smart；body 使用 ik_max_word
                    .properties("title", Property.of(p -> p.text(TextProperty.of(b -> b.analyzer("ik_max_word").searchAnalyzer("ik_smart")))))
                // body字段：文本类型，使用ik_max_word分词器
                    .properties("body", Property.of(p -> p.text(TextProperty.of(b -> b.analyzer("ik_max_word")))))
                // tags字段：关键字类型
                    .properties("tags", Property.of(p -> p.keyword(KeywordProperty.of(b -> b))))
                // author_id字段：长整型
                    .properties("author_id", Property.of(p -> p.long_(LongNumberProperty.of(b -> b))))
                // author_avatar字段：关键字类型
                    .properties("author_avatar", Property.of(p -> p.keyword(KeywordProperty.of(b -> b))))
                // author_nickname字段：关键字类型
                    .properties("author_nickname", Property.of(p -> p.keyword(KeywordProperty.of(b -> b))))
                // author_tag_json字段：关键字类型
                    .properties("author_tag_json", Property.of(p -> p.keyword(KeywordProperty.of(b -> b))))
                // publish_time字段：日期类型
                    .properties("publish_time", Property.of(p -> p.date(DateProperty.of(b -> b))))
                // like_count字段：整型
                    .properties("like_count", Property.of(p -> p.integer(IntegerNumberProperty.of(b -> b))))
                // favorite_count字段：整型
                    .properties("favorite_count", Property.of(p -> p.integer(IntegerNumberProperty.of(b -> b))))
                // view_count字段：整型
                    .properties("view_count", Property.of(p -> p.integer(IntegerNumberProperty.of(b -> b))))
                // status字段：关键字类型
                    .properties("status", Property.of(p -> p.keyword(KeywordProperty.of(b -> b))))
                // img_urls字段：关键字类型
                    .properties("img_urls", Property.of(p -> p.keyword(KeywordProperty.of(b -> b))))
                // is_top字段：关键字类型
                    .properties("is_top", Property.of(p -> p.keyword(KeywordProperty.of(b -> b))))
                // title_suggest字段：补全类型，用于搜索建议
                    .properties("title_suggest", Property.of(p -> p.completion(CompletionProperty.of(b -> b)))
                    )));
        } catch (Exception ignored) {
            // 忽略异常以保证应用启动；索引可能由后续写入动态创建，但 Mapping 将不完整
        }
    }
}