package com.amazonaws.iov;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TelemetryToIcebergJobTest {

    /**
     * 最关键的一条：Kafka 的 SASL 配置值内部含分号
     * （'...IAMLoginModule required;'）。朴素的 split(";") 会把 CREATE TABLE
     * 语句在中间截断，产出的 SQL 无法解析。
     */
    @Test
    void doesNotSplitOnSemicolonInsideQuotedLiteral() {
        String sql =
                "CREATE TABLE t (a INT) WITH ("
                        + "'properties.sasl.jaas.config' = "
                        + "'software.amazon.msk.auth.iam.IAMLoginModule required;'"
                        + ");\n"
                        + "INSERT INTO x SELECT * FROM t;";

        List<String> statements = TelemetryToIcebergJob.splitStatements(sql);

        assertEquals(2, statements.size(), "expected exactly 2 statements");
        assertTrue(statements.get(0).contains("IAMLoginModule required;"),
                "quoted semicolon must stay inside the first statement");
        assertTrue(statements.get(1).startsWith("INSERT"));
    }

    @Test
    void stripsLineComments() {
        String sql = "-- leading comment\nSELECT 1;\n-- trailing comment\nSELECT 2; -- inline\n";
        List<String> statements = TelemetryToIcebergJob.splitStatements(sql);
        assertEquals(2, statements.size());
        assertEquals("SELECT 1", statements.get(0));
        assertEquals("SELECT 2", statements.get(1));
    }

    @Test
    void doesNotTreatDoubleDashInsideLiteralAsComment() {
        String sql = "SELECT 'a--b' AS v;";
        List<String> statements = TelemetryToIcebergJob.splitStatements(sql);
        assertEquals(1, statements.size());
        assertEquals("SELECT 'a--b' AS v", statements.get(0));
    }

    @Test
    void handlesEscapedSingleQuoteInsideLiteral() {
        // SQL 中连续两个单引号表示一个字面量单引号，不应结束字符串
        String sql = "SELECT 'it''s; fine' AS v;";
        List<String> statements = TelemetryToIcebergJob.splitStatements(sql);
        assertEquals(1, statements.size());
        assertEquals("SELECT 'it''s; fine' AS v", statements.get(0));
    }

    @Test
    void rendersPlaceholders() {
        Map<String, String> values = new HashMap<>();
        values.put("kafka.topic", "vehicle-telemetry");
        String rendered = TelemetryToIcebergJob.renderPlaceholders("topic=${kafka.topic}", values);
        assertEquals("topic=vehicle-telemetry", rendered);
    }

    @Test
    void failsFastOnUnresolvedPlaceholder() {
        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> TelemetryToIcebergJob.renderPlaceholders("x=${missing.key}", new HashMap<>()));
        assertTrue(error.getMessage().contains("missing.key"));
    }

    /**
     * 用真实的 pipeline.sql 验证：2 条语句、占位符齐备、只有 1 条 INSERT。
     *
     * <p>catalog 不再由 SQL 的 CREATE CATALOG 声明（会触发 Managed Flink 上的
     * Hadoop 类加载失败），改由 registerIcebergCatalog() 以编程方式注册，
     * 因此这里应当只剩源表 DDL 与 INSERT 两条。
     */
    @Test
    void realPipelineSqlParsesIntoExpectedStatements() {
        String template = readResource("/sql/pipeline.sql");

        Map<String, String> config = new HashMap<>();
        config.put("kafka.topic", "vehicle-telemetry");
        config.put("kafka.bootstrap.servers", "b-1.example:9098");
        config.put("kafka.group.id", "iceberg-sink-dev");
        config.put("kafka.startup.mode", "latest-offset");
        config.put("glue.database", "vehicle_iot");
        config.put("iceberg.table", "vehicle_telemetry");

        // 与 main() 相同顺序：先切分剥离注释，再替换占位符。
        // 顺序颠倒会让注释中出现的 ${...} 之类文本被误当作占位符。
        List<String> statements = TelemetryToIcebergJob.splitStatements(template);
        assertEquals(2, statements.size(), "expected CREATE TABLE (source) + INSERT only");

        List<String> rendered = new ArrayList<>();
        for (String statement : statements) {
            rendered.add(TelemetryToIcebergJob.renderPlaceholders(statement, config));
        }

        for (String statement : rendered) {
            assertTrue(!statement.contains("${"),
                    "every placeholder must be covered by the runtime property set: " + statement);
        }
        assertTrue(rendered.get(0).startsWith("CREATE TABLE"));
        assertTrue(rendered.get(1).startsWith("INSERT INTO"));

        // SQL 里不应再出现 CREATE CATALOG——它由 Java 侧注册
        for (String statement : rendered) {
            assertTrue(!statement.toUpperCase(java.util.Locale.ROOT).contains("CREATE CATALOG"),
                    "CREATE CATALOG must not be in SQL; it fails on Managed Flink's classloader");
        }

        // 源表 DDL 必须完整保留 SASL 配置，包括其中的分号
        assertTrue(rendered.get(0).contains("IAMLoginModule required;"));
        assertTrue(rendered.get(0).contains("SASL_SSL"));
        // 占位符确实被注入了实际取值
        assertTrue(rendered.get(0).contains("vehicle-telemetry"));
        assertTrue(rendered.get(1).contains("vehicle_iot"));
    }

    private static String readResource(String name) {
        try (InputStream in = TelemetryToIcebergJobTest.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IllegalStateException("resource not found: " + name);
            }
            try (Scanner scanner = new Scanner(in, StandardCharsets.UTF_8.name())) {
                scanner.useDelimiter("\\A");
                return scanner.hasNext() ? scanner.next() : "";
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
