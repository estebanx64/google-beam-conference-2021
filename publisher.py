# Standard Library
import base64
import json

# Third Party Packages
from config.project_config import TOPIC
from faker import Faker
from google.cloud import pubsub_v1

fake = Faker()
publisher = pubsub_v1.PublisherClient()


def sent_pubsub_payloads(iterations: int):
    for _ in range(iterations):
        data = {
            "name": fake.name(),
            "company": fake.company(),
            "phone_number": f"+{fake.msisdn()}",
            "job": fake.job(),
            "msg": fake.sentence(),
            "remote_ip": fake.ipv4_public(),
            "user_agent": fake.user_agent(),
            "date": str(fake.date_between("today", "+8h")),
        }

        payload_string = json.dumps(data)
        payload_bytes = payload_string.encode("ascii")

        base64_bytes = base64.b64encode(payload_bytes)

        future = publisher.publish(TOPIC, base64_bytes)
        print(future.result())


if __name__ == "__main__":
    sent_pubsub_payloads(1)
