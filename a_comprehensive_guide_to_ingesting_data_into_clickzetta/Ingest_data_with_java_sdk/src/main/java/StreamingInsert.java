import com.clickzetta.client.ClickZettaClient;
import com.clickzetta.client.RealtimeStream;
import com.clickzetta.client.RowStream;
import com.clickzetta.platform.client.api.Options;
import com.clickzetta.platform.client.api.Row;
import com.clickzetta.platform.client.api.Stream;
import com.github.javafaker.Faker;
import org.json.JSONObject;
import org.apache.log4j.PropertyConfigurator;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.io.IOException;
import java.util.Locale;

public class StreamingInsert {
    private static ClickZettaClient client;
    private static String service;
    private static String instance;
    private static String password;
    private static String table = "lift_tuckets_import_by_java_sdk_realtime_ingest";
    private static String workspace;
    private static String schema;
    private static String vc;
    private static String user;
    static RealtimeStream realtimeStream;

    public static void main(String[] args) throws Exception {
        // 加载 log4j 配置文件
        PropertyConfigurator.configure("config/log4j.properties");

        // 读取配置文件
        String content = new String(Files.readAllBytes(Paths.get("config/config-ingest.json")));
        JSONObject config = new JSONObject(content);

        // 从 JSON 配置文件中获取值
        service = config.getString("service");
        instance = config.getString("instance");
        password = config.getString("password");
        workspace = config.getString("workspace");
        schema = config.getString("schema");
        vc = config.getString("vcluster");
        user = config.getString("username");

        // 初始化
        String url = MessageFormat.format("jdbc:clickzetta://{1}.{0}/{2}?" +
                        "schema={3}&username={4}&password={5}&virtualcluster={6}&",
                service, instance, workspace, schema, user, password, vc);
        Options options = Options.builder().build();
        client = ClickZettaClient.newBuilder().url(url).build();

        // 检查并创建目标表
        checkAndCreateTable(url);

        realtimeStream = client.newRealtimeStreamBuilder()
                .operate(RowStream.RealTimeOperate.CDC)
                .options(options)
                .schema(schema)
                .table(table)
                .build();

        Faker faker = new Faker(new Locale("zh", "CN"));
        String[] resorts = {"Resort 1", "Resort 2", "Resort 3"};
        Random random = new Random();
        int duration = 200;
        int maxRetries = 3;

        // 记录开始时间
        long startTime = System.currentTimeMillis();
        System.out.println("Start time: " + new Date(startTime));

        int totalRecords = 0;

        while (duration > 0) {
            for (int t = 1; t < 11; t++) {
                Row row = realtimeStream.createRow(Stream.Operator.UPSERT);
                row.setValue("txid", UUID.randomUUID().toString());
                row.setValue("rfid", Long.toHexString(random.nextLong() & ((1L << 96) - 1)));
                row.setValue("resort", faker.options().option(resorts));
                row.setValue("purchase_time", new Date().toString());
                row.setValue("expiration_time", new Date(System.currentTimeMillis() + 86400000L).toString());
                row.setValue("days", faker.number().numberBetween(1, 7));
                row.setValue("name", faker.name().fullName());
                row.setValue("address_street", faker.address().streetAddress());
                row.setValue("address_city", faker.address().city());
                row.setValue("address_state", faker.address().state());
                row.setValue("address_postalcode", faker.address().zipCode());
                row.setValue("phone", faker.phoneNumber().phoneNumber());
                row.setValue("email", faker.internet().emailAddress());
                row.setValue("emergency_contact_name", faker.name().fullName());
                row.setValue("emergency_contact_phone", faker.phoneNumber().phoneNumber());

                int attempts = 0;
                boolean success = false;
                while (attempts < maxRetries && !success) {
                    try {
                        realtimeStream.apply(row);
                        success = true;
                    } catch (IOException e) {
                        attempts++;
                        System.err.println("Attempt " + attempts + " failed: " + e.getMessage());
                        if (attempts == maxRetries) {
                            throw e;
                        }
                        Thread.sleep(1000); // 等待 1 秒后重试
                    }
                }
                totalRecords++;
            }
            Thread.sleep(200);
            duration = duration - 1;
        }

        realtimeStream.close();
        client.close();

        // 记录结束时间
        long endTime = System.currentTimeMillis();
        System.out.println("End time: " + new Date(endTime));

        // 计算平均每秒插入的记录数
        double elapsedTimeInSeconds = (endTime - startTime) / 1000.0;
        double recordsPerSecond = totalRecords / elapsedTimeInSeconds;
        System.out.println("Total records inserted: " + totalRecords);
        System.out.println("Elapsed time (seconds): " + elapsedTimeInSeconds);
        System.out.println("Average records per second: " + recordsPerSecond);
    }

    private static void checkAndCreateTable(String url) throws Exception {
        String checkTableSQL = "SELECT 1 FROM " + schema + "." + table + " LIMIT 1";
        String createTableSQL = "CREATE TABLE if not exists " + table + " (" +
                "`txid` string PRIMARY KEY," +
                "`rfid` string," +
                "`resort` string," +
                "`purchase_time` string," +
                "`expiration_time` string," +
                "`days` int," +
                "`name` string," +
                "`address_street` string," +
                "`address_city` string," +
                "`address_state` string," +
                "`address_postalcode` string," +
                "`phone` string," +
                "`email` string," +
                "`emergency_contact_name` string," +
                "`emergency_contact_phone` string);";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(checkTableSQL)) {
                // 如果表存在，什么也不做
            } catch (Exception e) {
                // 如果表不存在，创建表
                try (PreparedStatement pstmt = conn.prepareStatement(createTableSQL)) {
                    pstmt.executeUpdate();
                    System.out.println("Table created successfully.");
                }
            }
        }
    }
}
