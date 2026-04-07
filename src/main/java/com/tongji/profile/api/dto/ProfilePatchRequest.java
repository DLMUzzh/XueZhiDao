package com.tongji.profile.api.dto;

import jakarta.validation.constraints.PastOrPresent;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.LocalDate;

/**
 * 资料局部更新请求（PATCH）。
 * <p>
 * 客户端仅需提交欲更新的字段；未提交的字段保持不变。发送相同值的重复请求为幂等操作。
 */
public record ProfilePatchRequest(
        //{nickname: "测试用户", bio: "努力做个幸福的人", zgId: "aaabbb_12356", gender: "MALE", birthday: "2026-03-04"}
        //bio:"努力做个幸福的人"
        //birthday:"2026-03-04"
        //gender:"MALE"
        //nickname:"测试用户"
        //phone:"15931759680"
        //school:"大连海事大学"
        //zgId:"aaabbb_12356"
        @Size(min = 1, max = 64, message = "昵称长度需在 1-64 之间") String nickname,
        @Size(max = 512, message = "个人描述长度不能超过 512") String bio,
        @Pattern(regexp = "(?i)MALE|FEMALE|OTHER|UNKNOWN", message = "性别取值为 MALE/FEMALE/OTHER/UNKNOWN") String gender,
        @PastOrPresent(message = "生日不能晚于今天") LocalDate birthday,
        @Pattern(regexp = "^[a-zA-Z0-9_]{4,32}$", message = "知光号仅支持字母、数字、下划线，长度 4-32") String zgId,
        @Size(max = 128, message = "学校名称长度不能超过 128") String school,
        String tagJson
){ }