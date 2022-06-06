import csv
import calendar
import json
import os
import sys
import time

import requests


class WiFiUploader:
    def __init__(self):
        self.networks = {}

    def add_network(self, timestamp, lat, lon, mac_address, rssi):
        if timestamp not in self.networks.keys():
            self.networks[timestamp] = {"lat": lat, "lon": lon, "networks": []}

        self.networks[timestamp]["networks"].append({"bssid": mac_address, "rssi": rssi})

    def upload_networks(self):
        # Batch 100 networks
        net_counter = 0

        submission_positions = []
        for timestamp in self.networks.keys():
            position = {
                "position": {
                    "latitude": self.networks[timestamp]["lat"],
                    "longitude": self.networks[timestamp]["lon"],
                    "source": "gps",
                },
                "wifiAccessPoints": [
                    {
                        "macAddress": net["bssid"],
                        "signalStrength": net["rssi"]
                    } for net in self.networks[timestamp]["networks"]]
            }

            submission_positions.append(position)
            net_counter = net_counter + len(position["wifiAccessPoints"])

            if net_counter > 500:
                submission = {"items": submission_positions}
                print("Submitting {count} networks".format(count=net_counter))
                requests.post("https://location.services.mozilla.com/v2/geosubmit?key=test",
                              json.dumps(submission))
                net_counter = 0


def upload_from_csv(filepath: str):
    uploader = WiFiUploader()
    FIELD_NAMES = (
        "mac", "ssid", "auth_mode", "first_seen", "channel", "rssi", "lat", "lon", "altitude",
        "accuracy", "type")

    with open(filepath, 'r', errors='ignore') as input_file:
        reader = csv.DictReader((line.replace('\0', '') for line in input_file), fieldnames=FIELD_NAMES)

        # Skip meta + header information
        next(reader)
        next(reader)

        for line in reader:
            # Skip non-WiFi networks
            if line["type"] != "WIFI":
                continue

            uploader.add_network(calendar.timegm(time.strptime(line["first_seen"][:18], "%Y-%m-%d %H:%M:%S")),
                                 line["lat"], line["lon"], line["mac"], line["rssi"])

        uploader.upload_networks()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: {} <wigle-csv>".format(sys.argv[0]))
        sys.exit(1)

    upload_from_csv(sys.argv[1])
