"""
simulator/industrial_load.py

Large commercial or industrial load asset simulator.

Models a large grid-connected load (factory, data center, large commercial
building) that can island — disconnect from the grid and switch to backup
generation — to shed load during peak grid stress events.

Unlike thermostats (gradual, partial curtailment) this asset is binary:
either fully grid-connected at its current load, or fully islanded at zero
grid draw. This is called interruptible load service and is a real ERCOT
product (IL program) where large customers receive rate discounts in
exchange for agreeing to shed load on short notice.

Load varies by time of day following a typical commercial schedule —
high during business hours, reduced evenings and overnight.

Dispatch commands accepted:
    {"command": "island"}     - disconnect from grid (shed full load)
    {"command": "reconnect"}  - reconnect to grid (if min duration met)

State message fields:
    power_kw:               current grid draw (0.0 when islanded)
    charge_kw:              same as power_kw (for Grafana consistency)
    discharge_kw:           always 0.0 (load only asset)
    grid_connected:         1 if connected, 0 if islanded
    baseline_load_kw:       what load would be if grid connected right now
    mode:                   "normal" or "islanded"
    dispatch_active:        1 if responding to coordinator signal
    islanding_available:    1 if asset can island right now
    time_until_reconnect_sec: seconds remaining in minimum island duration

Real-world context:
    ERCOT's Interruptible Load (IL) program allows large customers to
    participate in ancillary services markets. During Energy Emergency
    Alerts (EEA), ERCOT can call on IL resources with as little as
    10 minutes notice. Participants are compensated through capacity
    payments and energy market credits.

    Common industrial load types in ERCOT territory:
    - Petrochemical plants along the Gulf Coast
    - Data centers (DFW, Austin corridors)
    - Large commercial buildings (hospitals, universities)
    - Water treatment facilities
    - Steel mills and manufacturing

    A single large industrial customer can represent 50-500 MW of
    interruptible capacity — equivalent to thousands of residential
    thermostats. This makes industrial DR one of the most cost-effective
    grid flexibility resources available.
"""

import random
import math
import time
from datetime import datetime, timezone
from simulator.asset_base import AssetBase
from grid.sim_clock import SimClock


class IndustrialLoadAsset(AssetBase):
    """
    Simulates a large commercial or industrial interruptible load.

    Models time-of-day load variation and binary islanding behavior.
    Enforces a minimum island duration to simulate real operational
    constraints — you cannot rapidly cycle a factory on and off.

    The coordinator should use islanding_available from the state message
    and treat this asset as a last resort after BESS, V2G, and thermostat
    curtailment have been exhausted.

    Args:
        asset_id:               Unique identifier, e.g. "industrial-01"
        peak_load_kw:           Maximum load during business hours (kW)
        min_load_kw:            Minimum load overnight (kW)
        min_island_duration_sec: Minimum time before reconnect allowed
                                 (default 300s = 5 min, represents 1hr real time)
    """

    def __init__(
        self,
        asset_id: str,
        peak_load_kw: float = 500.0,
        min_load_kw: float = 150.0,
        min_island_duration_sec: float = 300.0,
    ):
        super().__init__(asset_id=asset_id, asset_type="industrial_load")

        self.peak_load_kw = peak_load_kw
        self.min_load_kw = min_load_kw
        self.min_island_duration_sec = min_island_duration_sec

        self._grid_connected = True
        self._mode = "normal"
        self._dispatch_active = False

        # Timestamp when islanding began — used to enforce min duration
        self._island_start_time = None

        self.logger.info(
            f"Industrial load initialized: "
            f"peak={peak_load_kw}kW | min={min_load_kw}kW | "
            f"min island duration={min_island_duration_sec}s"
        )

    # ------------------------------------------------------------------
    # Load profile simulation
    # ------------------------------------------------------------------

    def _texas_hour(self) -> float:
        """Returns current hour in Texas local time (CDT = UTC-5)."""
        return SimClock.texas_hour()

    def _get_baseline_load_kw(self) -> float:
        """
        Returns the load this facility would draw if grid connected,
        based on time of day.

        Load profile shape:
            - Overnight (10pm-6am): min_load (skeleton crew, HVAC reduced)
            - Morning ramp (6am-9am): rises toward peak
            - Business hours (9am-6pm): peak load with variation
            - Evening ramp (6pm-10pm): declining toward minimum

        Adds small random noise to simulate real load variation
        (equipment cycling, variable process loads, etc.)
        """
        hour = SimClock.texas_hour()

        # Sine-based profile peaking at noon, troughing at midnight
        # Shifted and scaled to [min_load, peak_load] range
        phase = (hour - 6) / 24.0 * 2 * math.pi
        load_fraction = 0.5 * (1 + math.sin(phase))

        # Clamp overnight to minimum — industrial sites always have
        # some base load (security, refrigeration, standby systems)
        load_fraction = max(load_fraction, self.min_load_kw / self.peak_load_kw)

        baseline = self.min_load_kw + load_fraction * (
            self.peak_load_kw - self.min_load_kw
        )

        # Add ~2% noise for load variation
        noise = baseline * random.uniform(-0.02, 0.02)
        return round(baseline + noise, 2)

    def _get_time_until_reconnect_sec(self) -> float:
        """
        Returns seconds remaining before reconnection is allowed.
        Returns 0.0 if not islanded or minimum duration has elapsed.
        """
        if not self._island_start_time or self._grid_connected:
            return 0.0

        elapsed = time.monotonic() - self._island_start_time
        remaining = self.min_island_duration_sec - elapsed

        return round(max(0.0, remaining), 1)

    def _is_islanding_available(self) -> bool:
        """
        Returns True if this asset can island right now.
        Cannot island if already islanded.
        """
        return self._grid_connected

    # ------------------------------------------------------------------
    # AssetBase interface implementation
    # ------------------------------------------------------------------

    def get_state(self) -> dict:
        """
        Returns current industrial load state.

        power_kw is 0.0 when islanded — the facility is running on
        backup generation and drawing nothing from the grid.
        baseline_load_kw shows what the load would be if connected,
        so the coordinator can see the full impact of islanding.
        """
        baseline_load_kw = self._get_baseline_load_kw()
        power_kw = baseline_load_kw if self._grid_connected else 0.0
        time_until_reconnect = self._get_time_until_reconnect_sec()

        return {
            "power_kw":                 power_kw,
            "charge_kw":                power_kw,
            "discharge_kw":             0.0,
            "grid_connected":           1 if self._grid_connected else 0,
            "baseline_load_kw":         baseline_load_kw,
            "mode":                     self._mode,
            "dispatch_active":          1 if self._dispatch_active else 0,
            "islanding_available":      1 if self._is_islanding_available() else 0,
            "time_until_reconnect_sec": time_until_reconnect,
        }

    def on_dispatch(self, signal: dict) -> None:
        """
        Responds to coordinator dispatch signals.

        Accepted commands:
            {"command": "island"}     - shed full load immediately
            {"command": "reconnect"}  - return to grid if min duration met

        The coordinator should:
        - Check islanding_available before sending "island"
        - Check time_until_reconnect_sec == 0 before sending "reconnect"
        - Treat this asset as last resort after BESS, V2G, and thermostats
        """
        command = signal.get("command")

        if command == "island":
            if not self._grid_connected:
                self.logger.warning("Island command ignored — already islanded")
                return

            self._grid_connected = False
            self._mode = "islanded"
            self._dispatch_active = True
            self._island_start_time = time.monotonic()

            self.logger.info(
                f"Dispatched: islanding — shedding full load | "
                f"min reconnect in {self.min_island_duration_sec}s"
            )

        elif command == "reconnect":
            if self._grid_connected:
                self.logger.warning("Reconnect command ignored — already connected")
                return

            remaining = self._get_time_until_reconnect_sec()
            if remaining > 0:
                self.logger.warning(
                    f"Reconnect denied — minimum island duration not met "
                    f"({remaining:.0f}s remaining)"
                )
                return

            self._grid_connected = True
            self._mode = "normal"
            self._dispatch_active = False
            self._island_start_time = None

            self.logger.info("Dispatched: reconnecting to grid")

        else:
            self.logger.warning(f"Unknown command: {command}")