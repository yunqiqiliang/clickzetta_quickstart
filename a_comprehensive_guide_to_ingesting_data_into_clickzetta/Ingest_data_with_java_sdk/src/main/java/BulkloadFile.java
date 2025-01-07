import com.clickzetta.client.BulkloadStream;
import com.clickzetta.client.ClickZettaClient;
import com.clickzetta.client.RowStream;
import com.clickzetta.client.StreamState;
import com.clickzetta.platform.client.api.Row;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.log4j.PropertyConfigurator;

public class BulkloadFile {
    private static ClickZettaClient client;
    private static String service;
    private static String instance;
    private static String password;
    private static String table = "lift_tuckets_import_by_java_sdk_bulkload";
    private static String workspace;
    private static String schema;
    private static String vc;
    private static String user;
    static BulkloadStream bulkloadStream;

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
        initialize();

        // 统计文件里的行数
        int fileLineCount = countFileLines("data/lift_tickets_data.csv");
        System.out.println("Total lines in file: " + fileLineCount);

        // 创建表
        createTable();

        // 如果目标表存在，删除表里的数据
        deleteTableData();

        // 插入数据
        insertData();

        // 检查表中数据行数
        int tableRowCount = countTableRows();
        System.out.println("Total rows in table: " + tableRowCount);

        // 比较文件里的行数和表里的行数
        if (fileLineCount == tableRowCount) {
            System.out.println("Data inserted successfully! The row count matches.");
        } else {
            System.out.println("Data insertion failed! The row count does not match.");
        }

        // 关闭客户端
        client.close();
    }

    private static void initialize() throws Exception {
        String url = MessageFormat.format("jdbc:clickzetta://{1}.{0}/{2}?" +
                        "schema={3}&username={4}&password={5}&virtualcluster={6}&",
                service, instance, workspace, schema, user, password, vc);
        client = ClickZettaClient.newBuilder().url(url).build();
    }

    private static int countFileLines(String filePath) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            int lines = 0;
            while (reader.readLine() != null) lines++;
            return lines - 1; // 减去标题行
        }
    }

    private static void createTable() throws Exception {
        String url = MessageFormat.format("jdbc:clickzetta://{1}.{0}/{2}?" +
                        "schema={3}&username={4}&password={5}&virtualcluster={6}&",
                service, instance, workspace, schema, user, password, vc);

        String createTableSQL = "CREATE TABLE if not exists " + table + " (" +
                                "`txid` string," +
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
             PreparedStatement pstmt = conn.prepareStatement(createTableSQL)) {
            pstmt.executeUpdate();
            System.out.println("Table created successfully.");
        } catch (Exception e) {
            // 忽略该错误并继续
            System.out.println("Ignoring exception: " + e.getMessage());
        }
    }

    private static void deleteTableData() throws Exception {
        String url = MessageFormat.format("jdbc:clickzetta://{1}.{0}/{2}?" +
                        "schema={3}&username={4}&password={5}&virtualcluster={6}&",
                service, instance, workspace, schema, user, password, vc);

        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + schema + "." + table)) {
            pstmt.executeUpdate();
            System.out.println("Data deleted successfully from table: " + table);
        }
    }

    private static void insertData() throws Exception {
        bulkloadStream = client.newBulkloadStreamBuilder().schema(schema).table(table)
                .operate(RowStream.BulkLoadOperate.APPEND)
                .build();

        File csvFile = new File("data/lift_tickets_data.csv");
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        // 跳过标题行
        reader.readLine(); // Skip the first line (header)

        // 插入数据到数据库
        String line;
        while ((line = reader.readLine()) != null) {
            String[] values = line.split(",");
            // 类型转换保持和服务端类型一致
            String id = values[0]; // ID 是字符串类型
            String contentValue = values[1];
            Row row = bulkloadStream.createRow();
            // 设置参数值
            row.setValue(0, id);
            row.setValue(1, contentValue);
            // 必须调用该方法，否则无法将数据发送到服务端
            bulkloadStream.apply(row);
        }

        // 关闭资源
        reader.close();
        bulkloadStream.close();
        waitForBulkloadCompletion();
    }

    private static int countTableRows() throws Exception {
        String url = MessageFormat.format("jdbc:clickzetta://{1}.{0}/{2}?" +
                        "schema={3}&username={4}&password={5}&virtualcluster={6}&",
                service, instance, workspace, schema, user, password, vc);

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            String countSQL = "SELECT COUNT(*) FROM " + schema + "." + table;
            try (ResultSet rs = stmt.executeQuery(countSQL)) {
                if (rs.next()) {
                    return rs.getInt(1);
                } else {
                    throw new Exception("Failed to count table rows.");
                }
            }
        }
    }

    private static void waitForBulkloadCompletion() throws InterruptedException {
        while (bulkloadStream.getState() == StreamState.RUNNING) {
            Thread.sleep(1000);
        }
        if (bulkloadStream.getState() == StreamState.FAILED) {
            throw new ArithmeticException(bulkloadStream.getErrorMessage());
        }
    }
}
