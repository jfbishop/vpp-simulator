#!/bin/bash
# reset_data.sh
# Clears all simulation data from InfluxDB so the next run starts clean

echo "Clearing InfluxDB simulation data..."

docker exec -it vpp-simulator-influxdb-1 influx bucket delete \
  --name vpp_metrics \
  --org vpp \
  --token my-super-secret-token

docker exec -it vpp-simulator-influxdb-1 influx bucket create \
  --name vpp_metrics \
  --org vpp \
  --retention 0 \
  --org vpp \
  --token my-super-secret-token

echo "Done. You can now run python run_simulation.py"