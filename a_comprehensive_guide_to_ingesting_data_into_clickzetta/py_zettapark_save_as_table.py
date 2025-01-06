import json
import gzip
from clickzetta.zettapark.session import Session
from datetime import datetime

# 从配置文件中读取参数
with open('config-ingest.json', 'r') as config_file:
    config = json.load(config_file)

print("正在连接到云器Lakehouse.....\n")

# 创建会话
session = Session.builder.configs(config).create()

print("连接成功！...\n")

target_table_name = "lift_tuckets_import_by_py_save_as_table"

def save_as_table_to_clickzetta(session, schema, data):
    print('Saving data to Clickzetta Lakehouse')

    # Convert data to dataframe
    df = session.create_dataframe(data, schema=schema)
    
    # Save dataframe as table
    df.write.save_as_table(target_table_name, mode="overwrite", table_type="transient")
    print(f"Data saved to table {target_table_name}")

if __name__ == "__main__":
    schema = None
    data = []

    # 打开压缩的 JSON 文件并读取内容
    with gzip.open('lift_tickets_data.json.gz', 'rt', encoding='utf-8') as file:
        for message in file:
            if message.strip():  # 确保不是空行
                record = json.loads(message)
                if 'schema' in record:
                    schema = record['schema']
                else:
                    data.append(record)
    
    save_as_table_to_clickzetta(session, schema, data)
    session.close()
    print("Ingest complete")
