import psycopg2
import os
def load_csv_to_postgres(csv_file, table_name):
    with open(csv_file, 'r') as f:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", f)
    conn.commit()
# 数据库连接信息
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)
cur = conn.cursor()
# 设置 search_path 
cur.execute("SET search_path TO ingest_demo;")

# Clear all data from the lift_tickets_data table 
# cur.execute("TRUNCATE lift_tickets_data;")


# 定义CSV文件所在目录
csv_directory = 'data'

# 按表的依赖关系排列的CSV文件列表
csv_files = [
    "lift_tickets_data.csv",         # 首先导入被依赖的表
    "weather_data.csv",
    "lift_usage_data.csv",
    "feedback_data.csv",
    "incident_reports.csv",
    "accommodation_data.csv"
]

# 遍历文件列表并加载到对应的表中
for filename in csv_files:
    csv_file = os.path.join(csv_directory, filename)
    table_name = os.path.splitext(filename)[0]  # 文件名去掉扩展名作为表名
    print(f"Loading {csv_file} into table {table_name}...")
    load_csv_to_postgres(csv_file, table_name)
    print(f"Loaded {csv_file} into table {table_name} successfully!")

# Execute the SELECT query to count the rows in the table 
cur.execute("SELECT count(*) FROM lift_tickets_data;")
count = cur.fetchone()[0] 
# Print the result 
print(f"Total number of records in lift_tickets_data: {count}")
# 关闭游标和连接
cur.close()
conn.close()