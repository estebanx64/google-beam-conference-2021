import re
import base64
import json
from functools import reduce

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery

from user_agents import parse
import phonenumbers
from config.project_config import PROJECT, BUCKET, TOPIC, TABLE_SCHEMA, TABLE_COMPLETE_NAME


def parse_ascii_message(data):
    return json.loads(data.decode("ascii"))


def parse_phone_number(data):
    try:
        phone_parsed = phonenumbers.parse(data["phone_number"])

        country_code = str(phone_parsed.country_code) if phone_parsed.country_code else ""
        national_number = str(phone_parsed.national_number) if phone_parsed.national_number else ""
        extension = str(phone_parsed.extension) if phone_parsed.extension else ""

        data["phone_info"] = {
            "country_code": country_code,
            "national_number": national_number,
            "extension": extension,
        }

        return data
    except Exception:
        data["phone_info"] = {}
        return data


class GeoIp(beam.DoFn):
    geo_ip = None

    def deep_dictionary_search(self, dictionary: dict, keys: list):
        return reduce(lambda d, key: d.get(key) if isinstance(d, dict) else None, keys, dictionary)

    def process(self, data):
        try:
            if self.geo_ip is None:
                from resources.loader import load_geoip
                self.geo_ip = load_geoip()

            iso_code = self.geo_ip.country(data["remote_ip"]).country.iso_code
            country = self.geo_ip.country(data["remote_ip"]).country.name

            geo_dict = {
                "iso_code": iso_code,
                "country": country
            }

            data["ip_info"] = geo_dict
        except Exception:
            data["ip_info"] = {}

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
        '--job_name=demogitex',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        '--requirements_file=requirements_pipe.txt',
        '--setup_file=./setup.py',
        '--runner=DataflowRunner',
        '--streaming',
        "--service_account_email=demo-gitex@demos-304101.iam.gserviceaccount.com",
    ]

    p = beam.Pipeline(argv=argv)

    (
        p | "Read from pubsub" >> ReadFromPubSub(topic=TOPIC)
        | "Decode base64 message" >> beam.Map(lambda msg: base64.b64decode(msg))
        | "Parse ascii and load message into a object" >> beam.Map(parse_ascii_message)
        | "Parse phone number and format info" >> beam.Map(parse_phone_number)
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
