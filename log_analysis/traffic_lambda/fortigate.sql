INSERT INTO network_log.traffic
SELECT
    CAST(date_time_raw AS TIMESTAMP) AS generated_time,
    device_name,
    source_ip AS source_address,
    source_port,
    destination_ip AS destination_address,
    destination_port,
    action,
    CAST(sentbyte AS INT) AS bytes_sent,
    CAST(rcvdbyte AS INT) AS bytes_received,
    CAST(duration AS INT) AS elapsed_time,
    DATEADD(
        second, CAST(duration AS INT) * -1, CAST(generated_time AS TIMESTAMP)
    ) AS start_time
FROM network_log.fortigate;