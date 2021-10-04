PROJECT = "demos-304101"
TOPIC_NAME = "demo-beam"
DATASET = "demobeam"
TABLE_NAME = "users_data"
BUCKET = "demo-beam-bucket-es"

TOPIC = f"projects/{PROJECT}/topics/{TOPIC_NAME}"

TABLE_COMPLETE_NAME = f"{PROJECT}:{DATASET}.{TABLE_NAME}"

TABLE_SCHEMA = {"fields": [
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "company", "type": "STRING", "mode": "NULLABLE"},
    {"name": "msg", "type": "STRING", "mode": "NULLABLE"},
    {"name": "remote_ip", "type": "STRING", "mode": "NULLABLE"},
    {"name": "user_agent", "type": "STRING", "mode": "NULLABLE"},
    {"name": "date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "device", "type": "RECORD", "mode": "NULLABLE",
        "fields": [
            {"name": "device", "type": "STRING", "mode": "NULLABLE"},
            {"name": "os", "type": "STRING", "mode": "NULLABLE"},
            {"name": "browser", "type": "STRING", "mode": "NULLABLE"}
        ]},
]}
