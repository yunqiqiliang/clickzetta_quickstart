import com.clickzetta.client.ClickZettaClient;
import com.clickzetta.client.RealtimeStream;
import com.clickzetta.client.RowStream;
import com.clickzetta.platform.client.api.Options;
import com.clickzetta.platform.client.api.Row;
import com.clickzetta.platform.client.api.Stream;
import java.util.Random;
import java.util.Date;
import java.text.MessageFormat;

public class streaming_insert {
    private static ClickZettaClient client;
    private static String service;
    private static String instance;
    private static String password;
    private static String table = "target_table";
    private static String workspace;
    private static String schema;
    private static String vc;
    private static String user;
    static RealtimeStream realtimeStream;

    public static void main(String[] args) throws Exception {
        String content = new String(Files.readAllBytes(Paths.get("../../../config/config-ingest.json")));
        JSONObject config = new JSONObject(content);

        // 从 JSON 配置文件中获取值
        service = config.getString("service");
        instance = config.getString("instance");
        password = config.getString("password");
        workspace = config.getString("workspace");
        schema = config.getString("schema");
        vc = config.getString("vcluster");
        user = config.getString("username");
        String url = MessageFormat.format("jdbc:clickzetta://{0}/{1}?" +
                        "schema={2}&username={3}&password={4}&virtualcluster={5}&",
                service, instance, workspace, schema, user, password, vc);
        Options options = Options.builder().build();
        client = ClickZettaClient.newBuilder().url(url).build();
        realtimeStream = client.newRealtimeStreamBuilder()
                .operate(RowStream.RealTimeOperate.APPEND_ONLY)
                .options(options)
                .schema(schema)
                .table(table)
                .build();
        Random random = new Random();
        int duration = 200;
        System.out.println(new Date());
        while (duration > 0) {
            for (int t = 1; t < 11; t++) {
                Row row = realtimeStream.createRow(Stream.Operator.INSERT);
                row.setValue("lo_orderkey", random.nextInt(999999) + 100000);
                row.setValue("lo_linenumber", t);
                row.setValue("lo_custkey", 14176);
                row.setValue("lo_partkey", 189480);
                row.setValue("lo_suppkey", 1084);
                row.setValue("lo_orderdate", 1000);
                row.setValue("lo_orderpriority", "1-URGENT");
                row.setValue("lo_shippriority", 0);
                row.setValue("lo_quantity", 40);
                row.setValue("lo_extendedprice", 7219608);
                row.setValue("lo_ordtotalprice", 20503679);
                row.setValue("lo_discount", 2);
                row.setValue("lo_revenue", 7075215);
                row.setValue("lo_supplycost", 94168);
                row.setValue("lo_tax", 5);
                row.setValue("lo_commitdate", 1000);
                row.setValue("lo_shipmode", "MAIL");
                row.setValue("c_name", "Nick");
                row.setValue("c_address", "fkQworxl");
                row.setValue("c_city", "INDIA    4");
                row.setValue("c_nation", "INDIA");
                row.setValue("c_region", "ASIA");
                row.setValue("c_phone", "18-508-209-2090");
                row.setValue("c_mktsegment", "AUTOMOBILE");
                row.setValue("s_name", "Supplier#000001084");
                row.setValue("s_address", "qicitWA");
                row.setValue("s_city", "ARGENTINA6");
                row.setValue("s_nation", "ARGENTINA");
                row.setValue("s_region", "AMERICA");
                row.setValue("s_phone", "11-378-899-7136");
                row.setValue("p_name", "khaki cyan");
                row.setValue("p_mfgr", "MFGR#3");
                row.setValue("p_category", "MFGR#33");
                row.setValue("p_brand", "MFGR#3310");
                row.setValue("p_color", "lace");
                row.setValue("p_type", "MEDIUM BRUSHED TIN");
                row.setValue("p_size", 31);
                row.setValue("p_container", "MED CASE");
                realtimeStream.apply(row);
            }
            Thread.sleep(200);
            duration = duration - 1;
        }
        realtimeStream.close();
        client.close();
        System.out.println(new Date());
    }
}
