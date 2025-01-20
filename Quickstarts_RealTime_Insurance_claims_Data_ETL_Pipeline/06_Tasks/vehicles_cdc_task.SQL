MERGE INTO vehicles AS a
    USING stream_vehicles AS b
    ON a.vehicle_id = b.vehicle_id
    WHEN NOT MATCHED AND b.__change_type = 'INSERT' THEN
        INSERT (vehicle_id, vehicle_age, fuel_type, parking_sensors, parking_camera, rear_brakes_type, displacement, trasmission_type, steering_type, turning_radius, gross_weight, front_fog_lights, rear_window_wiper, rear_window_washer, rear_window_defogger, brake_assist, central_locking, power_steering, day_night_rear_view_mirror, is_speed_alert, ncap_rating)
        VALUES (b.vehicle_id, b.vehicle_age,b.fuel_type, b.parking_sensors, b.parking_camera, b.rear_brakes_type, b.displacement, b.trasmission_type, b.steering_type, b.turning_radius, b.gross_weight, b.front_fog_lights, b.rear_window_wiper, b.rear_window_washer, b.rear_window_defogger, b.brake_assist, b.central_locking, b.power_steering, b.day_night_rear_view_mirror, b.is_speed_alert, b.ncap_rating);