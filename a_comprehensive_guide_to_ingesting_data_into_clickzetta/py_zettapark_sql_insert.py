import json,gzip
from clickzetta.zettapark.session import Session
from datetime import datetime

# 从配置文件中读取参数
with open('config-ingest.json', 'r') as config_file:
    config = json.load(config_file)

print("正在连接到云器Lakehouse.....\n")

# 创建会话
session = Session.builder.configs(config).create()

print("连接成功！...\n")

target_table_name = "lift_tuckets_import_by_py_insert"

create_target_table_query = f"""
CREATE TABLE if not exists ql_ws.ingest.{target_table_name}(
  `txid` string,
  `rfid` string,
  `resort` string,
  `purchase_time` timestamp_ltz,
  `expiration_time` date,
  `days` int,
  `name` string,
  `address_street` string,
  `address_city` string,
  `address_state` string,
  `address_postalcode` string,
  `phone` string,
  `email` string,
  `emergency_contact_name` string,
  `emergency_contact_phone` string)
"""
session.sql(create_target_table_query).collect()

def save_to_clickzetta(session, message):
    record = json.loads(message)
    print('inserting record to Clickzetta Lakehouse')

    # 转换日期和时间字段
    purchase_time = datetime.strptime(record['purchase_time'], '%Y-%m-%d %H:%M:%S')
    expiration_time = datetime.strptime(record['expiration_time'], '%Y-%m-%d').date()

    row = (
        f"'{record['txid']}'", f"'{record['rfid']}'", f"'{record['resort']}'", 
        f"timestamp_ltz '{record['purchase_time']}'", f"date '{record['expiration_time']}'", 
        record['days'], f"'{record['name']}'", f"'{record['address_street']}'", 
        f"'{record['address_city']}'", f"'{record['address_state']}'", 
        record['address_postalcode'], record['phone'], 
        f"'{record['email']}'", f"'{record['emergency_contact_name']}'", 
        record['emergency_contact_phone']
    )

    sql_query = f"""
    INSERT INTO ql_ws.ingest.{target_table_name} 
    (TXID, RFID, RESORT, PURCHASE_TIME, EXPIRATION_TIME, DAYS, NAME, ADDRESS_STREET, ADDRESS_CITY, ADDRESS_STATE, ADDRESS_POSTALCODE, PHONE, EMAIL, EMERGENCY_CONTACT_NAME, EMERGENCY_CONTACT_PHONE) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    session.sql(sql_query, row).collect()
    print(f"inserted ticket {record}")

if __name__ == "__main__":
    # 打开 JSON 文件并读取内容
    with gzip.open('lift_tickets_data.json.gz', 'rt', encoding='utf-8') as file:
        for message in file:
            if message.strip():  # 确保不是空行
                save_to_clickzetta(session, message)
    
    session.close()
    print("Ingest complete")
