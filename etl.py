import re
import base64
import json
from functools import reduce

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

from user_agents import parse
from geolite2 import geolite2
from config.project_config import PROJECT, BUCKET, TOPIC, TABLE_SCHEMA, TABLE_COMPLETE_NAME


def parse_ascii_message(data):
    return json.loads(data.decode("ascii"))


class GeoIp(beam.DoFn):

    def deep_dictionary_search(self, dictionary: dict, keys: list):
        return reduce(lambda d, key: d.get(key) if isinstance(d, dict) else None, keys, dictionary)

    def get_geo_ip(self, data):
        reader = geolite2.reader()
        try:
            geo = reader.get(data["remote_ip"])
        except Exception:
            data["ip_info"] = {}
            return data
        finally:
            geolite2.close()

        geo_dict = {
            "continent": self.deep_dictionary_search(geo, ["continent", "names", "en"]),
            "country": self.deep_dictionary_search(geo, ["country", "names", "en"]),
            "city": self.deep_dictionary_search(geo, ["city", "names", "en"])
        }
        return geo_dict

    def process(self, data):
        location = self.get_geo_ip(data)
        data["ip_info"] = location
        return [data]


class GetUserAgent(beam.DoFn):

    def parse_user_agent(self, user_agent):
        try:
            ua_parse = parse(user_agent)
            ua_properties = str(ua_parse).split("/")

            os = ua_properties[1][1:]
            browser = ua_properties[2][1:]

            return {
                'device': ua_properties[0][:-1],
                'os': re.findall('^[^\d]*', os)[0][:-1],
                'browser': re.findall('^[^\d]*', browser)[0][:-1]
            }
        except Exception:
            return {}

    def process(self, data):
        user_agent = self.parse_user_agent(data["user_agent"])
        data["device"] = user_agent

        return [data]


def run():
    argv = [
        '--project={}'.format(PROJECT),
        "--region=us-east1",
        '--job_name=demodemo',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        '--requirements_file=requirements_pipe.txt',
        '--runner=DataflowRunner',
        '--streaming',
        '--service_account_email=todo-839@demos-304101.iam.gserviceaccount.com'
    ]

    p = beam.Pipeline(argv=argv)

    (
        p | "Read from pubsub" >> ReadFromPubSub(topic=TOPIC)
        | "Decode base64 message" >> beam.Map(lambda msg: base64.b64decode(msg))
        | "Parse ascii and load message into a object" >> beam.Map(parse_ascii_message)
        | "Locate ip address" >> beam.ParDo(GeoIp())
        | "Get user agent info" >> beam.ParDo(GetUserAgent())
        | "Write in bigquery" >> WriteToBigQuery(
            table=TABLE_COMPLETE_NAME,
            schema=TABLE_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )

    p.run().wait_until_finish()


if __name__ == "__main__":
    run()
