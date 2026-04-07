package com.tongji.llm.service.impl;

import com.tongji.llm.service.KnowPostDescriptionService;
import com.tongji.common.exception.BusinessException;
import com.tongji.common.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.deepseek.DeepSeekChatOptions;
import org.springframework.stereotype.Service;

import java.text.Normalizer;

@Service
@RequiredArgsConstructor
public class KnowPostDescriptionServiceImpl implements KnowPostDescriptionService {

    private final ChatClient chatClient;

    /**
     * 基于正文生成不超过 50 字的中文描述。
     *
     * @param content 输入的正文内容，不能为空或空白字符串。
     * @return 生成的中文描述，长度不超过 50 个汉字。
     * @throws BusinessException 如果输入内容为空，则抛出业务异常，错误码为 BAD_REQUEST。
     * @throws BusinessException 如果大模型调用失败，则抛出业务异常，错误码为 INTERNAL_ERROR。
     */
    public String generateDescription(String content) {
        // 检查输入内容是否为空或仅包含空白字符
        if (content == null || content.trim().isEmpty()) {
            throw new BusinessException(ErrorCode.BAD_REQUEST, "正文内容不能为空");
        }
        // 定义系统提示语，指导大模型生成符合要求的描述
        String system = "你是中文文案编辑。请基于用户提供的知文正文，生成一个中文描述，简洁有吸引力，且不超过50个汉字。不输出解释或多段，只输出结果。";
        // 构造用户输入内容，将正文包装成标准格式
        String user = "正文如下：\n\n" + content + "\n\n请直接给出不超过50字的中文描述。";
        try {
            // 调用大模型生成描述
            String result = chatClient
                    .prompt()                          // 创建一个新的对话提示
                    .system(system)                    // 设置系统角色指令
                    .user(user)                        // 设置用户输入内容
                    .options(DeepSeekChatOptions.builder()  // 配置模型参数
                            .model("deepseek-chat")         // 使用 deepseek-chat 模型
                            .temperature(0.8)               // 控制生成文本的随机性（0.8 表示较高质量但有一定多样性）
                            .maxTokens(120)                 // 最大生成 token 数量
                            .build())
                    .call()                             // 发起调用
                    .content();                         // 获取生成结果

            // 对生成的结果进行后处理，确保符合格式和长度要求
            return postProcess(result);
        } catch (Exception e) {
            // 捕获异常并转换为业务异常，便于统一处理
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "大模型调用失败: " + e.getMessage());
        }
    }

    /**
     * 对生成的文本进行后处理，包括标准化、清理和截断。
     *
     * @param text 大模型生成的原始文本，可能包含换行符、多余空格或标点符号。
     * @return 处理后的文本，满足以下条件：
     *         - 标准化 Unicode 编码；
     *         - 移除多余的换行符和空格；
     *         - 去除首尾引号和多余标点；
     *         - 截断至最多 50 个汉字。
     */
    private String postProcess(String text) {
        // 如果输入为空，直接返回空字符串
        if (text == null) {
            return "";
        }
        // 标准化 Unicode 编码（NFKC 形式），统一字符表示
        String t = Normalizer.normalize(text, Normalizer.Form.NFKC)
                .replaceAll("\r\n|\r|\n", " ")      // 将换行符替换为空格
                .replaceAll("\\s+", " ")           // 合并多个连续空格为单个空格
                .trim();                           // 去除首尾空格
        // 去除首尾可能出现的引号（如双引号、单引号、中文引号等）
        t = t.replaceAll("^[\"'“”‘’]+|[\"'“”‘’]+$", "")
                .replaceAll("[。!！?？；;、]+$", "");   // 去除末尾多余的标点符号
        // 按照 Unicode code point 计算字符数，避免中英文混合导致的长度误判
        int limit = 50;                            // 最大字符数限制
        int count = t.codePointCount(0, t.length()); // 实际字符数
        if (count <= limit) {
            return t;                              // 若未超限，直接返回
        }
        // 若超过限制，逐字符截断至指定长度
        StringBuilder sb = new StringBuilder();
        int i = 0, added = 0;
        while (i < t.length() && added < limit) {
            int cp = t.codePointAt(i);             // 获取当前字符的 code point
            sb.appendCodePoint(cp);                // 添加到结果中
            i += Character.charCount(cp);          // 更新索引位置
            added++;                               // 已添加字符数加一
        }
        return sb.toString();                      // 返回截断后的字符串
    }
}
