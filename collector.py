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
    last_measurement: dict[str, float] = dict()
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
        print("RAWR")

        if not self.last_measurement:
            return

        gauge = GaugeMetricFamily("tfa_sensor_temperature", "TFA WeatherHub Sensors", labels=["deviceid"])

        for deviceid, temperature in self.last_measurement.items():
            gauge.add_metric([deviceid], temperature)

        yield gauge


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
            response = requests.post(self.BASE_URI, data=request_data)

            if response.status_code == 429:
                self.ratelimit_backoff()
                continue
            
            if response.status_code != 200:
                logging.error(f"Failed to fetch measurements code status code: {response.status_code}")
                continue
            
            devices = response.json()["devices"]

            for device in devices:
                self.last_measurement[device["deviceid"]] = device["measurement"]["t1"]

            logging.info(self.last_measurement)


    def ratelimit_backoff(self):
        logging.warning("We hit the API ratelimit")
        # API documentation says will block for 7 minutes here, so wait 8 minutes before allow requests
        self.last_request_time = datetime.now() + timedelta(minutes=8)

    
    def __del__(self):
        self.running = False
        self.api_thread = None


def fetch_settings() -> Settings:
    with open("settings.yaml") as file:
        data = yaml.safe_load(file)
        return Settings(data["phone_id"], data["sensor_ids"])
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("><>< Starting up TFA collector ><><")
    
    settings = fetch_settings()

    collector = TFACollector(settings)
    collector.start()

    print("HLLO")

    registry = CollectorRegistry()
    registry.register(collector)
    handler = MetricsHandler.factory(registry)
    httpd = exposition.ThreadingWSGIServer(("", 9009), handler)

    print(httpd)
    httpd.serve_forever()
    