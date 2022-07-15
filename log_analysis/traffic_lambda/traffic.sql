CREATE TABLE IF NOT EXISTS network_log.traffic (
    generated_time TIMESTAMP,
    device_name VARCHAR,
    source_address VARCHAR,
    source_port VARCHAR,
    destination_address VARCHAR,
    destination_port VARCHAR,
    action VARCHAR,
    bytes_sent INT,
    bytes_received INT,
    elapsed_time INT,
    start_time TIMESTAMP
) DISTSTYLE AUTO SORTKEY AUTO ENCODE AUTO;