package com.github.im.client;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 追加日志（JSON Lines）实现的本地消息表：<b>一行一条记录，后写覆盖先写</b>。
 *
 * <h2>为什么是追加日志，不是「每次改都重写整个文件」</h2>
 *
 * <p>聊天记录的行数只增不减。如果每次 ack 都重写全表，写放大就是 O(n²)：
 * 一万条历史时，仅仅收一条消息就要写一万行。追加是 O(1)，
 * 而「重复行」的代价可以用 compaction 定期回收。
 *
 * <h2>为什么不做 fsync</h2>
 *
 * <p>{@code flush()} 只保证数据进了操作系统页缓存，进程被杀不会丢（这正是我们要防的场景），
 * 只有<b>整机掉电</b>才可能丢最后几条。为一个移动端场景每条消息付一次 fsync（几十 ms）
 * 是不划算的：真掉电时，服务端还有 seq 补发这一层兜底。
 *
 * <h2>compaction 的触发条件</h2>
 *
 * <p>{@code 日志行数 > 2 × 活记录数}（且有下限保护）。2 倍意味着平均每条记录只被改写过一次
 * （一条消息典型就两次：PENDING → SENT），超过这个比例说明日志里一半以上是废行。
 * 重写是「写临时文件 + 原子 move」，因此<b>任何时刻磁盘上都有一个完整可用的日志</b>——
 * 中途被杀的结果是「还是旧日志」，而不是「表被截断了」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class FileLocalMessageTable extends InMemoryLocalMessageTable {

    private static final System.Logger log = System.getLogger(FileLocalMessageTable.class.getName());
    /** 日志行数低于这个值时不做 compaction：几百行的日志重写不如放着 */
    private static final long COMPACT_MIN_LINES = 1024;

    private final Path journal;
    private final ObjectMapper mapper = new ObjectMapper();

    private BufferedWriter writer;
    private long writtenLines;
    private boolean closed;

    /**
     * 按「目录 + 用户 + 设备」派生日志文件，避免同一台机器上多账号互相踩表。
     *
     * <p>文件名里保留 userId/deviceId 是刻意的：排查问题时一眼能看出这个文件属于谁，
     * 不用再去翻配置。字符做最小替换，避免 Windows 上非法文件名。
     */
    public static FileLocalMessageTable forDevice(Path directory, String userId, String deviceId)
            throws IOException {
        String user = sanitize(userId);
        String device = sanitize(deviceId == null || deviceId.isBlank() ? "default" : deviceId);
        return new FileLocalMessageTable(directory.resolve("im-" + user + '-' + device + ".messages.jsonl"));
    }

    private static String sanitize(String value) {
        return value == null ? "unknown" : value.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    /**
     * @param journal 日志文件路径（父目录不存在会自动创建；文件不存在视为空表）
     */
    public FileLocalMessageTable(Path journal) throws IOException {
        this.journal = journal.toAbsolutePath();
        Path parent = this.journal.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        load();
        this.writer = Files.newBufferedWriter(this.journal,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
    }

    /** 读日志回放索引；坏行跳过而不是让整个表打不开（一行损坏不该毁掉全部聊天记录） */
    private void load() throws IOException {
        if (!Files.exists(journal)) {
            return;
        }
        long lines = 0;
        long broken = 0;
        try (Stream<String> stream = Files.lines(journal, StandardCharsets.UTF_8)) {
            for (String line : (Iterable<String>) stream::iterator) {
                if (line.isBlank()) {
                    continue;
                }
                lines++;
                try {
                    restore(fromJson(mapper.readTree(line)));
                } catch (RuntimeException e) {
                    broken++;
                }
            }
        }
        this.writtenLines = lines;
        if (broken > 0) {
            log.log(System.Logger.Level.WARNING,
                    "[local-table] {0} 行日志无法解析，已跳过（文件：{1}）", broken, journal);
        }
    }

    @Override
    protected void afterMutation(LocalMessage message) {
        if (closed) {
            return;
        }
        try {
            writer.write(toJson(message));
            writer.newLine();
            writer.flush();
            writtenLines++;
            maybeCompact();
        } catch (IOException e) {
            // 落盘失败不打断发送：内存索引仍然正确，只是这次进程重启会丢最后几条，
            // 而「丢几条」由服务端的 seq 补发兜底。抛出去反而会让一条 IO 抖动毁掉整条连接。
            log.log(System.Logger.Level.WARNING, "[local-table] 追加日志失败：" + e.getMessage(), e);
        }
    }

    private void maybeCompact() {
        if (writtenLines <= COMPACT_MIN_LINES || writtenLines <= 2L * size()) {
            return;
        }
        compact();
    }

    private void compact() {
        Path temp = journal.resolveSibling(journal.getFileName() + ".compact");
        try {
            List<LocalMessage> rows = snapshot();
            try (BufferedWriter out = Files.newBufferedWriter(temp, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                for (LocalMessage row : rows) {
                    out.write(toJson(row));
                    out.newLine();
                }
            }
            writer.close();
            Files.move(temp, journal,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            writer = Files.newBufferedWriter(journal, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
            writtenLines = rows.size();
            log.log(System.Logger.Level.DEBUG,
                    "[local-table] compaction 完成：{0} 行 → {1} 条记录", writtenLines, rows.size());
        } catch (IOException e) {
            log.log(System.Logger.Level.WARNING, "[local-table] compaction 失败（保留旧日志）：" + e.getMessage(), e);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            writer.close();
        } catch (IOException e) {
            log.log(System.Logger.Level.WARNING, "[local-table] 关闭日志失败：" + e.getMessage(), e);
        }
    }

    // ==== 行序列化 ====

    private String toJson(LocalMessage message) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("clientMsgId", message.clientMsgId());
        row.put("direction", message.direction().name());
        row.put("status", message.status().name());
        row.put("conversationId", message.conversationId());
        row.put("convType", message.convType());
        row.put("from", message.from());
        row.put("to", message.to());
        row.put("body", message.body());
        row.put("contentType", message.contentType());
        row.put("attrs", message.attrs());
        row.put("traceId", message.traceId());
        row.put("clientTs", message.clientTs());
        row.put("serverMsgId", message.serverMsgId());
        row.put("seq", message.seq());
        row.put("attempts", message.attempts());
        row.put("nextAttemptAt", message.nextAttemptAt());
        row.put("lastError", message.lastError());
        row.put("createdAt", message.createdAt());
        row.put("updatedAt", message.updatedAt());
        return mapper.writeValueAsString(row);
    }

    private LocalMessage fromJson(JsonNode node) {
        return new LocalMessage(
                text(node, "clientMsgId"),
                LocalMessage.Direction.valueOf(text(node, "direction", "OUT")),
                LocalMessage.Status.valueOf(text(node, "status", "PENDING")),
                text(node, "conversationId"),
                text(node, "convType"),
                text(node, "from"),
                text(node, "to"),
                text(node, "body"),
                text(node, "contentType"),
                text(node, "attrs"),
                text(node, "traceId"),
                node.path("clientTs").asLong(0),
                text(node, "serverMsgId"),
                node.path("seq").asLong(0),
                node.path("attempts").asInt(0),
                node.path("nextAttemptAt").asLong(0),
                text(node, "lastError"),
                node.path("createdAt").asLong(0),
                node.path("updatedAt").asLong(0));
    }

    private static String text(JsonNode node, String field) {
        return text(node, field, null);
    }

    private static String text(JsonNode node, String field, String fallback) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return fallback;
        }
        return value.asString();
    }
}
