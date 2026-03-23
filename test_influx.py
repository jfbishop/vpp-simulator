from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import os
from datetime import datetime, timezone

load_dotenv()

URL = os.getenv("INFLUXDB_URL")
TOKEN = os.getenv("INFLUXDB_TOKEN")
ORG = os.getenv("INFLUXDB_ORG")
BUCKET = os.getenv("INFLUXDB_BUCKET")

client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Write a test data point
point = (
    Point("asset_state")           # this is the "measurement" name, like a table name
    .tag("asset_id", "test-asset")
    .tag("asset_type", "test")
    .field("power_kw", 42.5)
    .time(datetime.now(timezone.utc))
)

write_api.write(bucket=BUCKET, org=ORG, record=point)
print("Write successful!")

# Read it back
query_api = client.query_api()
query = '''
from(bucket: "vpp_metrics")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "asset_state")
'''

tables = query_api.query(query)
for table in tables:
    for record in table.records:
        print(f"Read back: {record.get_field()} = {record.get_value()} | asset_id: {record.values.get('asset_id')}")

client.close()
print("Done!")