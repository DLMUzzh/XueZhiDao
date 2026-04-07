package com.tongji.relation.outbox;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.core.task.TaskExecutor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Canal→Kafka 桥接器。
 * 职责：订阅 outbox 表的行级变更（ROWDATA），仅转发 INSERT/UPDATE 的 payload 字段到 Kafka 主题；批次确认位点确保至少一次语义。
 * 可靠性：解析失败或非关心类型不提交位点；停止时断开 Canal 连接并清理资源。
 */
//SmartLifecycle 接口允许组件在 Spring 容器启动和关闭时自动执行特定逻辑。start() 和 stop() 方法分别用于启动和停止服务
@Service
public class CanalKafkaBridge implements SmartLifecycle {
    private final KafkaTemplate<String, String> kafka; // Kafka 模板，用于发送消息到 Kafka 主题
    private final ObjectMapper objectMapper; // JSON 序列化器，用于将对象转换为 JSON 字符串
    private final boolean enabled; // 是否启用桥接器
    private final String host; // Canal 服务器主机地址
    private final int port; // Canal 服务器端口
    private final String destination; // Canal 实例名
    private final String username; // Canal 用户名
    private final String password; // Canal 密码
    private final String filter; // 订阅过滤表达式，用于指定监听的表
    private final int batchSize; // 拉取批次大小，控制每次从 Canal 获取的数据量
    private final long intervalMs; // 空轮询间隔毫秒，当没有数据时的等待时间
    private volatile boolean running; // 桥接器是否正在运行
    private final TaskExecutor taskExecutor; // 任务执行器，用于异步执行主循环
    private CanalConnector connector; // Canal 连接器，用于与 Canal 服务器通信
    private static final Logger log = LoggerFactory.getLogger(CanalKafkaBridge.class); // 日志记录器

    /**
     * Canal 到 Kafka 的桥接器。
     * @param kafka Kafka 模板
     * @param objectMapper JSON 序列化器
     * @param enabled 是否启用
     * @param host Canal 主机
     * @param port Canal 端口
     * @param destination 实例名
     * @param username 用户名
     * @param password 密码
     * @param filter 订阅过滤表达式
     * @param batchSize 拉取批次大小
     * @param intervalMs 空轮询间隔毫秒
     */
    public CanalKafkaBridge(KafkaTemplate<String, String> kafka,
                            ObjectMapper objectMapper,
                            @Qualifier("taskExecutor") TaskExecutor taskExecutor,
                            @Value("${canal.enabled}") boolean enabled,
                            @Value("${canal.host}") String host,
                            @Value("${canal.port}") int port,
                            @Value("${canal.destination}") String destination,
                            @Value("${canal.username}") String username,
                            @Value("${canal.password}") String password,
                            @Value("${canal.filter}") String filter,
                            @Value("${canal.batchSize}") int batchSize,
                            @Value("${canal.intervalMs}") long intervalMs) {
        this.kafka = kafka;
        this.objectMapper = objectMapper;
        this.taskExecutor = taskExecutor;
        this.enabled = enabled;
        this.host = host;
        this.port = port;
        this.destination = destination;
        this.username = username;
        this.password = password;
        this.filter = filter;
        this.batchSize = batchSize;
        this.intervalMs = intervalMs;
    }

    /**
     * 启动桥接器：消费 Canal 并投递到 Kafka。
     */
    @Override
    public void start() {
        if (running) {
            log.info("Canal bridge start skipped: running={} enabled={} host={} port={} dest={} filter={}", running, enabled, host, port, destination, filter);
            return;
        }
        // 标记运行并使用全局线程池异步执行主循环
        running = true;
        //使用 TaskExecutor 异步执行主循环，避免阻塞主线程。
        taskExecutor.execute(() -> {
            try {
                // 创建 Canal 单实例连接器并建立连接
                connector = CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), destination, username, password);
                log.info("Canal connecting to {}:{} dest={} user={} filter={}", host, port, destination, username, filter);
                connector.connect();
                // 订阅过滤表达式，仅拉取关心的表（如 outbox）
                connector.subscribe(filter);
                // 回滚到上次确认位点，保证一致性处理
                connector.rollback();
                log.info("Canal connected and subscribed: host={} port={} dest={} filter={} batchSize={} intervalMs={}ms", host, port, destination, filter, batchSize, intervalMs);
                while (running) {
                    // 拉取一批未确认消息（不自动 ack）
                    Message message = connector.getWithoutAck(batchSize);
                    //获取批量ID
                    long batchId = message.getId();
                    // 空批次或心跳时，按间隔休眠并继续轮询
                    if (batchId == -1 || message.getEntries() == null || message.getEntries().isEmpty()) {
                        try {
                            Thread.sleep(intervalMs);
                        } catch (InterruptedException ignored) {}
                        continue;
                    }
                    //CanalEntry.Entry 表示一个数据变更条目，CanalEntry.RowChange 表示行级变更数据。
                    for (CanalEntry.Entry entry : message.getEntries()) {
                        // 仅处理行级数据变更事件
                        if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                            continue;
                        }
                        CanalEntry.RowChange rowChange;

                        try {
                            // 解析二进制为 RowChange（包含 INSERT/UPDATE 的行变更）
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (Exception e) {
                            continue;
                        }

                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 仅转发 INSERT/UPDATE 事件，忽略其他类型
                        if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE) {
                            continue;
                        }
                        ArrayNode dataArray = objectMapper.createArrayNode();

                        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                            ObjectNode rowNode = objectMapper.createObjectNode();
                            for (CanalEntry.Column col : rowData.getAfterColumnsList()) {
                                // 提取 payload 字段值（JSON 字符串），供下游消费
                                if ("payload".equalsIgnoreCase(col.getName())) {
                                    rowNode.put("payload", col.getValue());
                                }
                            }
                            dataArray.add(rowNode);
                        }

                        ObjectNode msgNode = objectMapper.createObjectNode();
                        msgNode.put("table", entry.getHeader().getTableName());
                        //鉴定mysql的操作的
                        msgNode.put("type", eventType == CanalEntry.EventType.INSERT ? "INSERT" : "UPDATE");
                        msgNode.set("data", dataArray);

                        try {
                            // 序列化并发送到 Kafka 主题（canal-outbox）
                            String json = objectMapper.writeValueAsString(msgNode);
                            kafka.send(OutboxTopics.CANAL_OUTBOX, json);
                        } catch (Exception ignored) {}
                    }
                    // 批次确认（推进位点），避免消息重放
                    connector.ack(batchId);
                }
            } catch (Exception e) {
                log.error("Canal bridge error", e);
            } finally {
                if (connector != null) {
                    // 断开 Canal 连接（资源清理）
                    try {
                        connector.disconnect();
                        log.info("Canal disconnected: dest={}", destination);
                    } catch (Exception ex) {
                        log.warn("Canal disconnect failed: dest={} err={}", destination, ex.getMessage());
                    }
                }
            }
        });
    }

    /**
     * 停止桥接器。
     */
    @Override
    public void stop() {
        running = false;
    }

    /**
     * 是否处于运行状态。
     * @return 运行状态
     */
    @Override
    public boolean isRunning() {
        return running;
    }
}
