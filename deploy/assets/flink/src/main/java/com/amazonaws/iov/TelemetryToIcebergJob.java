package com.amazonaws.iov;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 从 classpath 载入 Flink SQL 管道定义并执行。
 *
 * <p>设计意图：SQL 与 Java 分离。Topic、表名、warehouse 路径等全部通过 Managed Flink
 * 的运行时属性注入，运维调整参数只需改 CDK 配置重新部署，不必改 Java 代码。
 *
 * <p>checkpoint 间隔、并行度、快照策略均由 Managed Flink 的应用配置控制，
 * 代码里刻意不设置，避免两处配置互相覆盖造成困惑。
 */
public class TelemetryToIcebergJob {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryToIcebergJob.class);

    private static final String PROPERTY_GROUP = "AppConfig";
    private static final String SQL_RESOURCE = "/sql/pipeline.sql";
    private static final String CATALOG_NAME = "glue_catalog";
    private static final String GLUE_CATALOG_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog";
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z0-9_.\\-]+)}");

    public static void main(String[] args) throws Exception {
        Map<String, String> config = loadRuntimeConfig();
        LOG.info("Loaded {} runtime properties from group {}", config.size(), PROPERTY_GROUP);

        // 先切分再替换占位符：切分会剥离注释，这样注释里出现的 ${...} 之类文本
        // 不会被当成真的占位符去解析。
        List<String> statements = splitStatements(readResource(SQL_RESOURCE));
        if (statements.isEmpty()) {
            throw new IllegalStateException("No SQL statements found in " + SQL_RESOURCE);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        registerIcebergCatalog(tableEnv, config);

        StreamStatementSet statementSet = tableEnv.createStatementSet();
        int insertCount = 0;

        for (String raw : statements) {
            String statement = renderPlaceholders(raw, config);
            if (statement.toUpperCase(Locale.ROOT).startsWith("INSERT")) {
                LOG.info("Adding INSERT to statement set: {}", abbreviate(statement));
                statementSet.addInsertSql(statement);
                insertCount++;
            } else {
                LOG.info("Executing DDL: {}", abbreviate(statement));
                tableEnv.executeSql(statement);
            }
        }

        if (insertCount == 0) {
            throw new IllegalStateException(SQL_RESOURCE + " contains no INSERT statement; nothing to run");
        }

        // attachAsDataStream + env.execute 是 Managed Flink 上运行 Table API 作业的推荐方式
        statementSet.attachAsDataStream();
        env.execute(config.getOrDefault("job.name", "vehicle-telemetry-to-iceberg"));
    }

    /**
     * 以编程方式注册 Iceberg catalog，而不是用 SQL 的 {@code CREATE CATALOG}。
     *
     * <p><b>为什么必须这样做</b>（这是实际部署踩出来的坑，不是风格偏好）：
     * SQL 的 {@code CREATE CATALOG ... 'type'='iceberg'} 会走
     * {@code FlinkCatalogFactory.createCatalog}，它内部调用
     * {@code clusterHadoopConf()} -> Flink 的 {@code HadoopUtils}。
     * {@code HadoopUtils} 属于 flink-runtime，在 Managed Flink 上由
     * <em>父类加载器</em>加载；JVM 解析它方法签名里的
     * {@code org.apache.hadoop.conf.Configuration} 时会使用父类加载器，
     * 而 Managed Flink 的父类路径不含 Hadoop（启动日志明确写着
     * "No Hadoop Dependency available"）。结果是应用启动即抛
     * {@code NoClassDefFoundError: org/apache/hadoop/conf/Configuration}
     * 并退回 READY —— 把 Hadoop 打进 fat jar 也<em>无法</em>解决，
     * 因为需要它的那个类不在用户类加载器里。
     *
     * <p>这里的 {@code new Configuration(...)} 编译在本类中，由用户类加载器解析，
     * jar 内已包含 hadoop-client-api，因此可正常加载；随后把 Hadoop 配置显式
     * 交给 {@link CatalogLoader#custom}，整条路径不再触碰父类加载器里的
     * {@code HadoopUtils}。
     *
     * <p>传 {@code false} 表示不加载 core-default.xml / core-site.xml：
     * GlueCatalog 与 S3FileIO 都走 AWS SDK v2，不读 Hadoop 配置，
     * 因此没有加载默认资源的必要，也就少一处运行期依赖。
     */
    private static void registerIcebergCatalog(StreamTableEnvironment tableEnv, Map<String, String> config) {
        String warehouse = required(config, "warehouse.path");
        String database = required(config, "glue.database");

        Configuration hadoopConf = new Configuration(false);

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("warehouse", warehouse);
        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        CatalogLoader catalogLoader = CatalogLoader.custom(
                CATALOG_NAME, catalogProperties, hadoopConf, GLUE_CATALOG_IMPL);

        FlinkCatalog catalog = new FlinkCatalog(
                CATALOG_NAME,
                database,
                Namespace.empty(),
                catalogLoader,
                Collections.emptyMap(),
                true,   // 与 FlinkCatalogFactory 默认一致：开启 catalog 缓存
                -1L);   // -1 = 缓存不过期

        tableEnv.registerCatalog(CATALOG_NAME, catalog);
        LOG.info("Registered Iceberg catalog '{}' (warehouse={}, database={})",
                CATALOG_NAME, warehouse, database);
    }

    private static String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("Missing required runtime property: " + key);
        }
        return value;
    }

    /** 读取 Managed Flink 运行时属性组。 */
    private static Map<String, String> loadRuntimeConfig() throws IOException {
        Map<String, Properties> groups = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties props = groups.get(PROPERTY_GROUP);
        if (props == null || props.isEmpty()) {
            throw new IllegalStateException(
                    "Runtime property group '" + PROPERTY_GROUP + "' is missing or empty. "
                            + "On Managed Flink it is set by the CDK stack; for local runs pass it via "
                            + "the KinesisAnalytics local config file.");
        }
        Map<String, String> config = new HashMap<>();
        for (String name : props.stringPropertyNames()) {
            config.put(name, props.getProperty(name));
        }
        return config;
    }

    private static String readResource(String resource) throws IOException {
        try (InputStream in = TelemetryToIcebergJob.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Resource not found on classpath: " + resource);
            }
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int read;
            while ((read = in.read(chunk)) != -1) {
                buffer.write(chunk, 0, read);
            }
            return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    /** 替换 ${key}；任何未提供取值的占位符都直接失败，避免把坏 SQL 提交上去。 */
    static String renderPlaceholders(String template, Map<String, String> values) {
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuffer out = new StringBuffer();
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = values.get(key);
            if (value == null) {
                throw new IllegalStateException(
                        "SQL placeholder ${" + key + "} has no matching runtime property in group " + PROPERTY_GROUP);
            }
            matcher.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    /**
     * 按分号切分 SQL 语句，但要感知引号与注释。
     *
     * <p>不能简单 split(";")：Kafka 的 SASL 配置值本身就含分号
     * （'...IAMLoginModule required;'），朴素切分会把语句截断。
     */
    static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inLineComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = (i + 1 < sql.length()) ? sql.charAt(i + 1) : '\0';

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    current.append(c);
                }
                continue;
            }

            if (!inSingleQuote && c == '-' && next == '-') {
                inLineComment = true;
                i++;
                continue;
            }

            if (c == '\'') {
                // SQL 中连续两个单引号是转义的字面量单引号，不切换引号状态
                if (inSingleQuote && next == '\'') {
                    current.append(c).append(next);
                    i++;
                    continue;
                }
                inSingleQuote = !inSingleQuote;
                current.append(c);
                continue;
            }

            if (c == ';' && !inSingleQuote) {
                addIfNotBlank(statements, current);
                current.setLength(0);
                continue;
            }

            current.append(c);
        }
        addIfNotBlank(statements, current);
        return statements;
    }

    private static void addIfNotBlank(List<String> target, StringBuilder buffer) {
        String statement = buffer.toString().trim();
        if (!statement.isEmpty()) {
            target.add(statement);
        }
    }

    private static String abbreviate(String value) {
        String flat = value.replaceAll("\\s+", " ");
        return flat.length() <= 160 ? flat : flat.substring(0, 160) + "...";
    }
}
