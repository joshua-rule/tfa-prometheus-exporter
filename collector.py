import logging
import yaml
import requests

from time import sleep
from threading import Thread
from datetime import datetime, timedelta

from prometheus_client import CollectorRegistry, exposition, MetricsHandler
from prometheus_client.core import GaugeMetricFamily


class Settings:

    phone_id: str
    sensor_ids: list[str]

    def __init__(self, phone_id: str, sensor_ids: list[str]):
        if not phone_id:
            raise ValueError("phone_id needs to be defined in settings")
        if not sensor_ids:
            raise ValueError("sensor_ids need to be defined in settings")

        self.phone_id = phone_id
        self.sensor_ids = sensor_ids


"""
    Collects data for TFA weather sensors to expose to prometheus
    
    https://www.tfa-dostmann.de/en/

    API documentation: https://mobile-alerts.eu/info/public_server_api_documentation.pdf

    Rate limits are defined, so we need to adhere to those
"""
class TFACollector:

    BASE_URI = "https://www.data199.com/api/pv1/device/lastmeasurement"

    settings: Settings

    last_request_time: datetime
    last_measurement: dict[str, dict[str, float]] = dict()
    interval: timedelta
    api_thread: Thread
    running = False

    def __init__(self, settings: Settings):
        self.settings = settings
        self.interval = timedelta(minutes=1)


    def start(self):
        self.last_request_time = datetime.min
        self.running = True
        self.api_thread = Thread(target=self.__fetch_measurements, daemon=True)
        self.api_thread.start()


    def stop(self):
        self.running = False


    def collect(self):
        if not self.last_measurement:
            return

        temp_gauge = GaugeMetricFamily("tfa_sensor_temperature", "TFA WeatherHub Sensors", labels=["deviceid"])
        humidity_gauge = GaugeMetricFamily("tfa_sensor_humidity", "TFA WeatherHub Sensors", labels=["deviceid"])

        for deviceid, measurement in self.last_measurement.items():
            temp_gauge.add_metric([deviceid], measurement["temperature"])
            humidity_gauge.add_metric([deviceid], measurement["humidity"])

        yield temp_gauge
        yield humidity_gauge


    def __fetch_measurements(self):
        while self.running:
            now = datetime.now()

            if self.last_request_time + self.interval > now:
                sleep(15)
                continue
            
            self.last_request_time = now
            
            request_data = {
                "phoneid": self.settings.phone_id,
                "deviceids": ",".join(self.settings.sensor_ids)
            }

            try:
                response = requests.post(self.BASE_URI, data=request_data)

                if response.status_code == 429:
                    self.ratelimit_backoff()
                    continue
                
                if response.status_code != 200:
                    logging.error(f"Failed to fetch measurements code status code: {response.status_code}")
                    continue
                
                devices = response.json()["devices"]

                for device in devices:
                    self.last_measurement[device["deviceid"]] = {
                        "temperature": device["measurement"]["t1"],
                        "humidity": device["measurement"]["h"],
                    }

                logging.info(self.last_measurement)
            except ConnectionError as error:
                logging.error("Failed to connect to API", exec_info = error)
            except Exception as error:
                logging.error("Unhandled error getting measure data", exc_info=error)


    def ratelimit_backoff(self):
        logging.warning("We hit the API ratelimit")
        # API documentation says will block for 7 minutes here, so wait 8 minutes before allow requests
        self.last_request_time = datetime.now() + timedelta(minutes=8)

    
    def __del__(self):
        self.running = False
        self.api_thread = None


def fetch_settings() -> Settings:
    with open("/home/prometheus/tfa-prometheus-exporter/settings.yaml") as file:
        data = yaml.safe_load(file)
        return Settings(data["phone_id"], data["sensor_ids"])
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("><>< Starting up TFA collector ><><")
    
    settings = fetch_settings()

    collector = TFACollector(settings)
    collector.start()

    registry = CollectorRegistry()
    registry.register(collector)
    handler = MetricsHandler.factory(registry)
    httpd = exposition.ThreadingWSGIServer(("", 9009), handler)

    httpd.serve_forever()
    
