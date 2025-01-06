import os
import sys
import rapidjson as json
import optional_faker as _
import uuid
import random
import time
import psycopg2
from faker import Faker
from datetime import date, datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
fake = Faker('zh_CN')  # 使用中文区域
resorts = ["大董烤鸭", "京雅堂", "新荣记", "仿膳饭庄", "全聚德", 
           "利群烤鸭店", "鼎泰丰", "海底捞", "江苏会所", "店客店来",
           "周黑鸭", "夜上海", "香宫", "长安壹号", "翡翠餐厅", "北京饭店",
           "四川豆花饭庄", "海底捞火锅", "川办餐厅", "南门火锅",
           "胡同", "翠园", "利苑酒家", "御宝轩", "金鼎轩", 
           "外婆家", "大董", "顺峰海鲜酒家", "小龙坎火锅",
           "新大陆中餐厅", "京兆尹", "鼎泰丰（台湾）", "滇池来客", 
           "绿波廊", "南美时光"]

# Load database credentials from environment variables
DB_NAME = 'sample'
DB_USER = 'metabase'
DB_PASSWORD = 'metasample123'
DB_HOST = '172.17.1.220'
DB_PORT = '5436'

def connect_db():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

def random_date_in_2025():
    start_date = date(2025, 1, 1)
    end_date = date(2025, 12, 31)
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

def random_datetime_between(start_year, end_year):
    start_datetime = datetime(start_year, 1, 1)
    end_datetime = datetime(end_year, 12, 31, 23, 59, 59)
    random_seconds = random.randint(0, int((end_datetime - start_datetime).total_seconds()))
    return start_datetime + timedelta(seconds=random_seconds)

def insert_lift_ticket(cursor, lift_ticket):
    cursor.execute("""
        INSERT INTO ingest_demo.lift_tickets_data (txid, rfid, resort, purchase_time, expiration_time, days, name, address_street, address_city, address_state, address_postalcode, phone, email, emergency_contact_name, emergency_contact_phone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        lift_ticket['txid'], lift_ticket['rfid'], lift_ticket['resort'],
        lift_ticket['purchase_time'], lift_ticket['expiration_time'],
        lift_ticket['days'], lift_ticket['name'], lift_ticket['address_street'],
        lift_ticket['address_city'], lift_ticket['address_state'],
        lift_ticket['address_postalcode'], lift_ticket['phone'],
        lift_ticket['email'], lift_ticket['emergency_contact_name'],
        lift_ticket['emergency_contact_phone']
    ))

def generate_lift_ticket():
    global resorts, fake
    lift_ticket = {
        'txid': str(uuid.uuid4()),
        'rfid': hex(random.getrandbits(96)),
        'resort': fake.random_element(elements=resorts),
        'purchase_time': random_datetime_between(2021, 2024),
        'expiration_time': random_date_in_2025(),
        'days': fake.random_int(min=1, max=7),
        'name': fake.name(),
        'address_street': fake.street_address(),
        'address_city': fake.city(),
        'address_state': fake.province(),
        'address_postalcode': fake.postcode(),
        'phone': fake.phone_number(),
        'email': fake.email(),
        'emergency_contact_name': fake.name(),
        'emergency_contact_phone': fake.phone_number(),
    }
    return lift_ticket

def main(total_count, batch_size, sleep_time):
    conn = connect_db()
    cursor = conn.cursor()
    
    batch_data = []
    for _ in range(total_count):
        lift_ticket = generate_lift_ticket()
        batch_data.append(lift_ticket)
        
        if len(batch_data) >= batch_size:
            for ticket in batch_data:
                insert_lift_ticket(cursor, ticket)
            conn.commit()
            batch_data = []
            time.sleep(sleep_time)
    
    # Insert any remaining data
    if batch_data:
        for ticket in batch_data:
            insert_lift_ticket(cursor, ticket)
        conn.commit()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("请提供总行数, 每批次行数, 每批次休眠秒数。例如：python rt_data_generate_insert_into_pg.py 1000 100 10")
        sys.exit(1)
    
    total_count = int(sys.argv[1])
    batch_size = int(sys.argv[2])
    sleep_time = int(sys.argv[3])
    
    main(total_count, batch_size, sleep_time)
