PROJECT = "demos-304101"
TOPIC_NAME = "demo-gitex-global-topic"
DATASET = "demo_gitex"
TABLE_NAME = "users_data"
BUCKET = "demo-gitex-global-bucket"

TOPIC = f"projects/{PROJECT}/topics/{TOPIC_NAME}"

TABLE_COMPLETE_NAME = f"{PROJECT}:{DATASET}.{TABLE_NAME}"

TABLE_SCHEMA = {
    "fields": [
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "company", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone_number", "type": "STRING", "mode": "NULLABLE"},
        {"name": "job", "type": "STRING", "mode": "NULLABLE"},
        {"name": "msg", "type": "STRING", "mode": "NULLABLE"},
        {"name": "remote_ip", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_agent", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date", "type": "DATE", "mode": "NULLABLE"},
        {
            "name": "ip_info",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {"name": "iso_code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
        {
            "name": "device",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {"name": "device", "type": "STRING", "mode": "NULLABLE"},
                {"name": "os", "type": "STRING", "mode": "NULLABLE"},
                {"name": "browser", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
        {
            "name": "phone_info",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {"name": "country_code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "national_number", "type": "STRING", "mode": "NULLABLE"},
                {"name": "extension", "type": "STRING", "mode": "NULLABLE"},
            ],
        },
    ]
}
