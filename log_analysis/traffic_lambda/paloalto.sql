INSERT INTO network_log.traffic
SELECT 
    CAST(generated_time AS TIMESTAMP),
    device_name,
    source_address,
    source_port,
    destination_address,
    destination_port,
    action,
    CAST(bytes_sent AS INT),
    CAST(bytes_received AS INT),
    CAST(elapsed_time AS INT),
    CAST(start_time AS TIMESTAMP)
FROM network_log.paloalto;