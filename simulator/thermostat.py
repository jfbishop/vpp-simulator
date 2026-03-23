"""
simulator/thermostat.py

Residential smart thermostat asset simulator.

Models a home HVAC system that responds to coordinator curtailment
signals by adjusting the cooling setpoint. Load is driven by the
difference between outdoor temperature and indoor setpoint —
the harder the HVAC works to maintain comfort, the higher the draw.

Unlike BESS and EV charger, this is a load-only asset (no storage,
no export). Curtailment reduces load by raising the setpoint, which
causes the HVAC to run less frequently. The response is gradual due
to thermal lag — the house takes time to warm up.

Dispatch commands accepted:
    {"command": "curtail", "setpoint_offset_f": 4.0} - raise setpoint
    {"command": "normal"}                             - restore setpoint

State message fields:
    power_kw:          current HVAC draw (always positive or 0)
    charge_kw:         same as power_kw (for Grafana consistency)
    discharge_kw:      always 0.0 (load-only asset)
    setpoint_f:        current target indoor temperature
    current_temp_f:    simulated indoor temperature
    outdoor_temp_f:    simulated outdoor temperature
    hvac_stage:        "cooling", "idle"
    mode:              "normal" or "curtailed"
    dispatch_active:   1 if responding to coordinator signal

Real-world context:
    Smart thermostat demand response is one of the most mature VPP
    programs in the US. Utilities like Xcel Energy, Pacific Gas &
    Electric, and Austin Energy (ERCOT territory) run large enrolled
    thermostat fleets. During peak events, setpoints are nudged 2-4°F
    for 2-4 hours. Studies show customers rarely notice the difference
    in comfort but the aggregate load reduction can be significant —
    a fleet of 10,000 homes can shed 20-40 MW during peak events.

    Thermal lag is a key characteristic — unlike batteries which
    respond in milliseconds, thermostat curtailment takes 15-30 minutes
    to show full effect as indoor temperatures drift up. This makes
    thermostats better for sustained peak shaving than fast frequency
    response services.
"""

import random
import math
from datetime import datetime, timezone
from simulator.asset_base import AssetBase
from grid.sim_clock import SimClock


class ThermostatAsset(AssetBase):
    """
    Simulates a residential smart thermostat with demand response capability.

    Models outdoor temperature as a diurnal cycle (hot Texas summer day),
    indoor temperature as a lagged response to outdoor conditions and HVAC
    operation, and HVAC power draw as a function of cooling load.

    Args:
        asset_id:           Unique identifier, e.g. "thermostat-01"
        normal_setpoint_f:  Normal cooling setpoint in °F (default 72)
        hvac_capacity_kw:   Maximum HVAC draw in kW (default 3.5kW ~= 1 ton)
        home_size_factor:   Scales thermal mass and load (default 1.0)
    """

    # Base setpoint offset when curtailed — coordinator can override this
    DEFAULT_CURTAIL_OFFSET_F = 4.0

    def __init__(
        self,
        asset_id: str,
        normal_setpoint_f: float = 72.0,
        hvac_capacity_kw: float = 3.5,
        home_size_factor: float = 1.0,
    ):
        super().__init__(asset_id=asset_id, asset_type="thermostat")

        self.normal_setpoint_f = normal_setpoint_f
        self.hvac_capacity_kw = hvac_capacity_kw * home_size_factor
        self.home_size_factor = home_size_factor

        # Current active setpoint — changes on curtailment dispatch
        self._setpoint_f = normal_setpoint_f

        # Simulated indoor temperature — starts at setpoint
        self._indoor_temp_f = normal_setpoint_f

        self._mode = "normal"
        self._dispatch_active = False

        self.logger.info(
            f"Thermostat initialized: setpoint={normal_setpoint_f}°F "
            f"| capacity={self.hvac_capacity_kw:.1f}kW "
            f"| size factor={home_size_factor}"
        )

    # ------------------------------------------------------------------
    # Temperature simulation
    # ------------------------------------------------------------------

    def _texas_hour(self) -> float:
        """Returns current hour in Texas local time (CDT = UTC-5)."""
        return SimClock.texas_hour()

    def _get_outdoor_temp_f(self) -> float:
        """
        Simulates outdoor temperature as a diurnal cycle for a hot Texas
        summer day. Peaks around 3-4pm, coolest around 6am.

        Range: ~75°F overnight to ~98°F afternoon peak.
        Adds small random noise for realism.
        """
        hour = SimClock.texas_hour()

        # Sine curve: trough at hour 6, peak at hour 15
        phase = (hour - 6) / 24.0 * 2 * math.pi
        base_temp = 86.0 + 12.0 * math.sin(phase)

        noise = random.uniform(-1.0, 1.0)
        return round(base_temp + noise, 1)

    def _update_indoor_temp(self, outdoor_temp_f: float) -> None:
        """
        Updates simulated indoor temperature based on:
        - Heat infiltration from outdoors (thermal lag)
        - HVAC cooling effect when running

        Thermal lag is modeled as a slow drift toward outdoor temperature
        when HVAC is off, and a slow drift toward setpoint when HVAC is on.
        The rate of change is intentionally slow to simulate real building
        thermal mass — a house doesn't heat up or cool down instantly.

        infiltration_rate: how fast indoor temp drifts toward outdoor
        cooling_rate:      how fast HVAC pulls indoor temp toward setpoint
        """
        # Scale rates by simulated interval so thermal dynamics work correctly
        # at any time scale
        sim_interval_hours = self.PUBLISH_INTERVAL_SIM_SEC / 3600.0

        # How much the indoor temp drifts toward outdoor per interval
        # Small value = high thermal mass = slow response
        infiltration_rate = 0.05 * self.home_size_factor * sim_interval_hours

        # How much HVAC pulls temp toward setpoint per interval
        cooling_rate = 0.15 * self.home_size_factor * sim_interval_hours

        # Drift toward outdoor temperature (heat always infiltrates)
        self._indoor_temp_f += (
            outdoor_temp_f - self._indoor_temp_f
        ) * infiltration_rate

        # HVAC cooling effect — only runs when indoor > setpoint
        if self._indoor_temp_f > self._setpoint_f:
            self._indoor_temp_f -= (
                self._indoor_temp_f - self._setpoint_f
            ) * cooling_rate

        self._indoor_temp_f = round(self._indoor_temp_f, 2)

    def _compute_power(self, outdoor_temp_f: float) -> float:
        """
        Computes HVAC power draw based on cooling load.

        Power scales with how hard the HVAC is working — proportional
        to the gap between indoor temperature and setpoint, up to the
        unit's capacity. When indoor temp is at or below setpoint,
        HVAC is idle and draws no power.

        Returns:
            float — power in kW (always >= 0, load only asset)
        """
        if self._indoor_temp_f <= self._setpoint_f:
            return 0.0

        # Load fraction: how hard is the HVAC working?
        # Scales from 0 (at setpoint) to 1.0 (at capacity)
        # Full capacity when indoor is 5°F or more above setpoint
        temp_gap = self._indoor_temp_f - self._setpoint_f
        load_fraction = min(temp_gap / 5.0, 1.0)

        # Add small noise to simulate compressor cycling
        noise = random.uniform(-0.02, 0.02)
        power = self.hvac_capacity_kw * load_fraction + noise

        return round(max(0.0, power), 2)

    # ------------------------------------------------------------------
    # AssetBase interface implementation
    # ------------------------------------------------------------------

    def get_state(self) -> dict:
        """
        Returns current thermostat state. Called every publish interval.

        charge_kw always equals power_kw (load only asset).
        discharge_kw always 0.0.
        Both fields included for Grafana query consistency across asset types.
        """
        outdoor_temp_f = self._get_outdoor_temp_f()
        self._update_indoor_temp(outdoor_temp_f)
        power_kw = self._compute_power(outdoor_temp_f)

        hvac_stage = "cooling" if power_kw > 0 else "idle"

        return {
            "power_kw":         power_kw,
            "charge_kw":        power_kw,
            "discharge_kw":     0.0,
            "setpoint_f":       round(self._setpoint_f, 1),
            "current_temp_f":   self._indoor_temp_f,
            "outdoor_temp_f":   outdoor_temp_f,
            "hvac_stage":       hvac_stage,
            "hvac_capacity_kw": self.hvac_capacity_kw,
            "mode":             self._mode,
            "dispatch_active":  1 if self._dispatch_active else 0,
        }

    def on_dispatch(self, signal: dict) -> None:
        """
        Responds to coordinator curtailment signals.

        Accepted commands:
            {"command": "curtail", "setpoint_offset_f": 4.0}
            {"command": "normal"}

        Curtailment raises the setpoint, reducing how often the HVAC
        runs and therefore reducing load. The effect is gradual due to
        thermal lag — full load reduction takes several publish intervals.

        The coordinator should not expect instant load reduction — plan
        for a 15-30 minute ramp in a real deployment (faster in simulation
        due to compressed time intervals).
        """
        command = signal.get("command")

        if command == "curtail":
            offset = float(signal.get(
                "setpoint_offset_f",
                self.DEFAULT_CURTAIL_OFFSET_F
            ))
            self._setpoint_f = self.normal_setpoint_f + offset
            self._mode = "curtailed"
            self._dispatch_active = True
            self.logger.info(
                f"Dispatched: setpoint raised to {self._setpoint_f}°F "
                f"(+{offset}°F) | indoor: {self._indoor_temp_f}°F"
            )

        elif command == "normal":
            self._setpoint_f = self.normal_setpoint_f
            self._mode = "normal"
            self._dispatch_active = False
            self.logger.info(
                f"Dispatched: setpoint restored to {self._setpoint_f}°F"
            )

        else:
            self.logger.warning(f"Unknown command: {command}")