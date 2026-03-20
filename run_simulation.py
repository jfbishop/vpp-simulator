"""
run_simulation.py

Main entry point for the VPP simulator. Starts all asset simulators,
the grid baseline publisher, and the coordinator in separate threads.

Asset fleet:
    BESS:          2 x 200kW / 400kWh (2-hour duration)
    EV Chargers:   3 x 7.2kW Level 2 with V2G
    Thermostats:   4 x residential (3.5kW HVAC capacity)
    Industrial:    1 x 500kW peak interruptible load

Press Ctrl+C to stop all threads.
"""

import time
import threading
import logging

from simulator.bess import BessAsset
from simulator.ev_charger import EvChargerAsset
from simulator.thermostat import ThermostatAsset
from simulator.industrial_load import IndustrialLoadAsset
from coordinator.coordinator import Coordinator
from grid.publisher import run as run_grid_publisher
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)

BROKER = os.getenv("MQTT_BROKER", "localhost")
PORT = int(os.getenv("MQTT_PORT", 1883))


def start_thread(target, name: str, daemon: bool = True) -> threading.Thread:
    """Starts a function in a named daemon thread."""
    t = threading.Thread(target=target, name=name, daemon=daemon)
    t.start()
    return t


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════╗
║           VPP Simulator Starting             ║
║                                              ║
║  Assets:                                     ║
║    2x BESS (200kW / 400kWh each)             ║
║    3x EV Charger (7.2kW V2G)                 ║
║    4x Smart Thermostat (3.5kW HVAC)          ║
║    1x Industrial Load (500kW peak)           ║
║                                              ║
║  Press Ctrl+C to stop                        ║
╚══════════════════════════════════════════════╝
    """)

    threads = []

    # -- Grid baseline publisher --
    threads.append(start_thread(run_grid_publisher, "grid-publisher"))
    time.sleep(0.5)

    # -- BESS assets --
    for i, initial_soc in enumerate([0.8, 0.5], start=1):
        bess = BessAsset(
            asset_id=f"bess-0{i}",
            power_rating_kw=200.0,
            energy_rating_kwh=400.0,
            initial_soc=initial_soc,
        )
        threads.append(start_thread(bess.run, f"bess-0{i}"))

    # -- EV charger assets --
    # Different plug-in/departure times to simulate a realistic fleet
    ev_configs = [
        {"asset_id": "ev-01", "initial_soc": 0.3, "plugged_in_hour": 17, "departure_hour": 8},
        {"asset_id": "ev-02", "initial_soc": 0.5, "plugged_in_hour": 18, "departure_hour": 9},
        {"asset_id": "ev-03", "initial_soc": 0.6, "plugged_in_hour": 19, "departure_hour": 7},
    ]
    for cfg in ev_configs:
        ev = EvChargerAsset(
            asset_id=cfg["asset_id"],
            charge_rate_kw=7.2,
            battery_kwh=60.0,
            initial_soc=cfg["initial_soc"],
            driver_min_soc=0.3,
            plugged_in_hour=cfg["plugged_in_hour"],
            departure_hour=cfg["departure_hour"],
        )
        threads.append(start_thread(ev.run, cfg["asset_id"]))

    # -- Thermostat assets --
    # Different home sizes create load diversity in the fleet
    thermostat_configs = [
        {"asset_id": "thermostat-01", "normal_setpoint_f": 72.0, "home_size_factor": 1.0},
        {"asset_id": "thermostat-02", "normal_setpoint_f": 71.0, "home_size_factor": 1.3},
        {"asset_id": "thermostat-03", "normal_setpoint_f": 73.0, "home_size_factor": 0.8},
        {"asset_id": "thermostat-04", "normal_setpoint_f": 72.0, "home_size_factor": 1.5},
    ]
    for cfg in thermostat_configs:
        thermostat = ThermostatAsset(
            asset_id=cfg["asset_id"],
            normal_setpoint_f=cfg["normal_setpoint_f"],
            hvac_capacity_kw=3.5,
            home_size_factor=cfg["home_size_factor"],
        )
        threads.append(start_thread(thermostat.run, cfg["asset_id"]))

    # -- Industrial load --
    industrial = IndustrialLoadAsset(
        asset_id="industrial-01",
        peak_load_kw=500.0,
        min_load_kw=150.0,
        min_island_duration_sec=300.0,
    )
    threads.append(start_thread(industrial.run, "industrial-01"))

    time.sleep(1)

    # -- Coordinator (runs in main thread) --
    coordinator = Coordinator(broker=BROKER, port=PORT)

    try:
        coordinator.run()
    except KeyboardInterrupt:
        print("\nShutting down VPP simulator...")