CREATE SCHEMA if not exists ingest_demo;
SET search_path TO ingest_demo;

-- 删除滑雪票数据表
DROP TABLE IF EXISTS lift_tickets_data CASCADE;

-- 删除滑雪票使用数据表
DROP TABLE IF EXISTS lift_usage_data CASCADE;

-- 删除反馈数据表
DROP TABLE IF EXISTS feedback_data CASCADE;

-- 删除事件报告数据表
DROP TABLE IF EXISTS incident_reports CASCADE;

-- 删除天气数据表
DROP TABLE IF EXISTS weather_data CASCADE;

-- 删除住宿数据表
DROP TABLE IF EXISTS accommodation_data CASCADE;

-- 滑雪票数据表
CREATE TABLE lift_tickets_data (
    txid UUID PRIMARY KEY,  -- 交易ID，唯一标识每张滑雪票
    rfid VARCHAR(24),  -- 滑雪票的RFID编号
    resort VARCHAR(50),  -- 度假村名称
    purchase_time TIMESTAMP,  -- 购买时间
    expiration_time DATE,  -- 到期日期
    days INTEGER,  -- 有效天数
    name VARCHAR(100),  -- 购票人姓名
    address_street VARCHAR(100),  -- 街道地址
    address_city VARCHAR(50),  -- 城市
    address_state VARCHAR(50),  -- 省份
    address_postalcode VARCHAR(20),  -- 邮政编码
    phone VARCHAR(20),  -- 电话号码
    email VARCHAR(100),  -- 电子邮件
    emergency_contact_name VARCHAR(100),  -- 紧急联系人姓名
    emergency_contact_phone VARCHAR(20)  -- 紧急联系人电话号码
);

-- 滑雪票使用数据表
CREATE TABLE lift_usage_data (
    txid UUID,  -- 交易ID
    usage_time TIMESTAMP,  -- 使用时间
    lift_id INTEGER,  -- 升降椅编号
    PRIMARY KEY (txid, usage_time)  -- 复合主键，唯一标识每次使用记录
);

-- 反馈数据表
CREATE TABLE feedback_data (
    txid UUID,  -- 交易ID
    resort VARCHAR(50),  -- 度假村名称
    feedback_time TIMESTAMP,  -- 反馈时间
    rating INTEGER,  -- 评分
    comment TEXT,  -- 评论内容
    PRIMARY KEY (txid, feedback_time)  -- 复合主键，唯一标识每条反馈记录
);

-- 事件报告数据表
CREATE TABLE incident_reports (
    txid UUID,  -- 交易ID
    incident_time TIMESTAMP,  -- 事件时间
    incident_type VARCHAR(50),  -- 事件类型
    description TEXT,  -- 事件描述
    PRIMARY KEY (txid, incident_time)  -- 复合主键，唯一标识每条事件记录
);

-- 天气数据表
CREATE TABLE weather_data (
    resort VARCHAR(50),  -- 度假村名称
    date DATE,  -- 日期
    temperature FLOAT,  -- 温度
    condition VARCHAR(50),  -- 天气状况
    PRIMARY KEY (resort, date)  -- 复合主键，唯一标识每条天气记录
);

-- 住宿数据表
CREATE TABLE accommodation_data (
    txid UUID,  -- 交易ID
    hotel_name VARCHAR(100),  -- 酒店名称
    room_type VARCHAR(50),  -- 房间类型
    check_in TIMESTAMP,  -- 入住时间
    check_out TIMESTAMP,  -- 退房时间
    PRIMARY KEY (txid, check_in)  -- 复合主键，唯一标识每条住宿记录
);
