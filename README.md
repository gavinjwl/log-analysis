# Log Analysis

## 建立 Provisioned Redshift Cluster

開啟 Redshift Console 並選擇 Create Cluster

1. __Node type__: ra3.xlplus
1. __Number of nodes__: 1
1. __Admin user name__: awsuser
1. __Admin user password__: `<your-password>`
1. __Associated IAM roles__: 建立一個新的 IAM Role 並給予 ManagedPolicy: `AmazonS3FullAccess`

![create-provisioned-redshift](pictures/create-provisioned-redshift.png)

## 建立 Cloud9 Environment

1. 開啟 Cloud9 畫面
1. 建立 Cloud9 Environment

    ![create-cloud9-environment-1](pictures/create-cloud9-environment-1.png)

    ![create-cloud9-environment-2](pictures/create-cloud9-environment-2.png)

1. 開啟 Cloud9 終端機

    ![open-cloud9-environment-terminal](pictures/open-cloud9-environment-terminal.png)

1. 確認使用者權限和 AWS 設定

    ```bash
    aws sts get-caller-identity

    aws configure list
    ```

    ![check-cloud9-environment](pictures/check-cloud9-environment.png)

1. 安裝 CDK

    ```bash
    node --version

    cdk --version
    
    ### if do not show cdk version, install it
    # npm install -g --force aws-cdk
    ```

    ![check-cdk](pictures/check-cdk.png)

1. Clone Github Repo

    ```bash
    git clone <repo>
    ```

1. 更新 Python 依賴的函式庫並啟用 Python 虛擬環境 (venv)

    ```bash
    pip3 install poetry

    cd <repo>

    poetry update

    source .venv/bin/activate
    ```

1. 部署 AWS 環境

    ```bash
    cdk synth

    cdk deploy --all
    ```

## 建立 Redshift Schema 和 Tables

使用 Redshift Query Editor V2 並使用 admin user 連接 Redshift Cluster

建立 Redshift Schema 和 Tables

```sql
CREATE SCHEMA IF NOT EXISTS network_log;

-- fortigate
CREATE TABLE IF NOT EXISTS network_log.fortigate (
    date_time_raw VARCHAR,
    device_name VARCHAR,
    source_ip VARCHAR,
    source_port VARCHAR,
    destination_ip VARCHAR,
    destination_port VARCHAR,
    action VARCHAR,
    duration VARCHAR,
    sentbyte VARCHAR,
    rcvdbyte VARCHAR
)
DISTSTYLE AUTO
SORTKEY AUTO
ENCODE AUTO
;

-- paloalto
CREATE TABLE IF NOT EXISTS network_log.paloalto (
    future_use_1 VARCHAR,
    receive_time VARCHAR,
    serial_number VARCHAR,
    type VARCHAR,
    threat_content_type VARCHAR,
    future_use_2 VARCHAR,
    generated_time VARCHAR,
    source_address VARCHAR,
    destination_address VARCHAR,
    nat_source_ip VARCHAR,
    nat_destination_ip VARCHAR,
    rule_name VARCHAR,
    source_user VARCHAR,
    destination_user VARCHAR,
    application VARCHAR,
    virtual_system VARCHAR,
    source_zone VARCHAR,
    destination_zone VARCHAR,
    inbound_interface VARCHAR,
    outbound_interface VARCHAR,
    log_action VARCHAR,
    future_use_3 VARCHAR,
    session_id VARCHAR,
    repeat_count VARCHAR,
    source_port VARCHAR,
    destination_port VARCHAR,
    nat_source_port VARCHAR,
    nat_destination_port VARCHAR,
    flags VARCHAR,
    protocol VARCHAR,
    action VARCHAR,
    bytes VARCHAR,
    bytes_sent VARCHAR,
    bytes_received VARCHAR,
    packets VARCHAR,
    start_time VARCHAR,
    elapsed_time VARCHAR,
    category VARCHAR,
    future_use_4 VARCHAR,
    sequence_number VARCHAR,
    action_flags VARCHAR,
    source_location VARCHAR,
    destination_location VARCHAR,
    future_use_5 VARCHAR,
    packets_sent VARCHAR,
    packets_received VARCHAR,
    session_end_reason VARCHAR,
    device_group_hierarchy_level_1 VARCHAR,
    device_group_hierarchy_level_2 VARCHAR,
    device_group_hierarchy_level_3 VARCHAR,
    device_group_hierarchy_level_4 VARCHAR,
    virtual_system_name VARCHAR,
    device_name VARCHAR,
    action_source VARCHAR,
    source_vm_uuid VARCHAR,
    destination_vm_uuid VARCHAR,
    tunnel_id_imsi VARCHAR,
    monitor_tag_imei VARCHAR,
    parent_session_id VARCHAR,
    parent_start_time VARCHAR,
    tunnel_type VARCHAR,
    sctp_association_id VARCHAR,
    sctp_chunks VARCHAR,
    sctp_chunks_sent VARCHAR,
    sctp_chunks_received VARCHAR,
    rule_uuid VARCHAR,
    http_2_connection VARCHAR,
    app_flap_count VARCHAR,
    policy_id VARCHAR,
    link_switches VARCHAR,
    sd_wan_cluster VARCHAR,
    sd_wan_device_type VARCHAR,
    sd_wan_cluster_type VARCHAR,
    sd_wan_site VARCHAR,
    dynamic_user_group_name VARCHAR
)
DISTSTYLE AUTO
SORTKEY AUTO
ENCODE AUTO
;

-- event_monitor
CREATE TABLE IF NOT EXISTS network_log.event_monitor (
    event_time TIMESTAMPTZ,
    principal VARCHAR,
    statement_name VARCHAR,
    statement_id VARCHAR,
    state VARCHAR
)
DISTSTYLE AUTO
SORTKEY AUTO
ENCODE AUTO
;
```

## 建立 Redshift Users 並給予權限

使用 Redshift Query Editor V2 並使用 admin user 連接 Redshift Cluster

建立 Redshift Users 並給予權限

```sql
-- Grant Fortigate
CREATE USER "<role-name-from-cfn-output>" PASSWORD DISABLE;
GRANT ALL ON SCHEMA network_log TO "<role-name-from-cfn-output>";
GRANT ALL ON TABLE network_log.fortigate TO "<role-name-from-cfn-output>";

-- Grant Paloalto
CREATE USER '<role-name-from-cfn-output>' PASSWORD DISABLE;
GRANT ALL ON SCHEMA network_log TO "<role-name-from-cfn-output>";
GRANT ALL ON TABLE network_log.paloalto TO "<role-name-from-cfn-output>";

-- Grant Traffic
CREATE USER '<role-name-from-cfn-output>' PASSWORD DISABLE;
GRANT ALL ON SCHEMA network_log TO "<role-name-from-cfn-output>";
GRANT ALL ON TABLE network_log.traffic TO "<role-name-from-cfn-output>";

-- Grant Monitor
CREATE USER '<role-name-from-cfn-output>' PASSWORD DISABLE;
GRANT ALL ON SCHEMA network_log TO "<role-name-from-cfn-output>";
GRANT ALL ON TABLE network_log.event_monitor TO "<role-name-from-cfn-output>";
```

## Troubleshooting

- Lambda worker 成功執行但是 Redshift 無反應

    1. 檢查 STL_ERROR

        ```sql
        SELECT userid, process, pid, context, errcode, error, recordtime
        FROM STL_ERROR
        ORDER BY recordtime DESC
        ;
        ```

    1. 測試 Redshift User 權限

        ```sql
        SET SESSION AUTHORIZATION '';
        -- Do something
        SET SESSION AUTHORIZATION default;
        ```
