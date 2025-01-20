--- STAGING TABLES ---
--creating staging tables for each normalized tables created by data pipeline
CREATE  TABLE IF NOT EXISTS staging_policy(
    policy_id VARCHAR(15) ,
    subscription_length FLOAT,
    region_code VARCHAR(5),
    segment VARCHAR(10));
    
CREATE  TABLE IF NOT EXISTS staging_vehicles (
    vehicle_id VARCHAR(15) ,
    vehicle_age FLOAT,
    fuel_type VARCHAR(10),
    parking_sensors VARCHAR(5),
    parking_camera VARCHAR(5),
    rear_brakes_type VARCHAR(10),
    displacement INT,
    trasmission_type VARCHAR(20),
    steering_type VARCHAR(15),
    turning_radius FLOAT,
    gross_weight INT,
    front_fog_lights VARCHAR(5),
    rear_window_wiper VARCHAR(5), 
    rear_window_washer VARCHAR(5),
    rear_window_defogger VARCHAR(5), 
    brake_assist VARCHAR(5), 
    central_locking VARCHAR(5),
    power_steering VARCHAR(5),      
    day_night_rear_view_mirror VARCHAR(5),  
    is_speed_alert VARCHAR(5),
    ncap_rating INT);

CREATE  TABLE IF NOT EXISTS staging_customers(
    customer_id VARCHAR(20) ,
    customer_age INT,
    region_density INT);

CREATE  TABLE IF NOT EXISTS staging_claims(
    claim_id VARCHAR(20) ,
    claim_status INT);




--- FINAL TABLES ---
--creating final table to store transformed data captured by stream objects 
CREATE  TABLE IF NOT EXISTS policy(
    policy_id VARCHAR(15) ,
    subscription_length FLOAT,
    region_code VARCHAR(5),
    segment VARCHAR(10));

SELECT * FROM policy;
TRUNCATE TABLE policy;

CREATE  TABLE IF NOT EXISTS vehicles (
    vehicle_id VARCHAR(15) ,
    vehicle_age FLOAT,
    fuel_type VARCHAR(10),
    parking_sensors VARCHAR(5),
    parking_camera VARCHAR(5),
    rear_brakes_type VARCHAR(10),
    displacement INT,
    trasmission_type VARCHAR(20),
    steering_type VARCHAR(15),
    turning_radius FLOAT,
    gross_weight INT,
    front_fog_lights VARCHAR(5),
    rear_window_wiper VARCHAR(5), 
    rear_window_washer VARCHAR(5),
    rear_window_defogger VARCHAR(5), 
    brake_assist VARCHAR(5), 
    central_locking VARCHAR(5),
    power_steering VARCHAR(5),      
    day_night_rear_view_mirror VARCHAR(5),  
    is_speed_alert VARCHAR(5),
    ncap_rating INT);

CREATE  TABLE IF NOT EXISTS customers(
    customer_id VARCHAR(20) ,
    customer_age INT,
    region_density INT);

CREATE  TABLE IF NOT EXISTS claims(
    claim_id VARCHAR(20) ,
    claim_status INT);