"""
simulator/ev_charger.py

Electric Vehicle (EV) charger asset simulator with Vehicle-to-Grid (V2G)
capability.

Models a bidirectional EV charger that can:
  - Charge the vehicle from the grid (positive power_kw = load on grid)
  - Discharge the vehicle battery to the grid (negative power_kw = export)
  - Pause charging temporarily to shed load (smart/managed charging)
  - Respect driver preferences: minimum SoC and departure time

Key difference from BESS:
    A BESS is a pure grid asset — it does whatever the coordinator says.
    An EV has a human attached to it. The coordinator must respect the
    driver's minimum SoC and departure time, and can only use capacity
    above the driver's floor for V2G. Pausing charge is only safe if
    there is enough time remaining to reach driver_min_soc before departure.

Dispatch commands accepted:
    {"command": "charge",        "target_kw": 7.2}   - charge at target rate
    {"command": "stop_charge"}                        - pause charging (load shed)
    {"command": "discharge",     "target_kw": 7.2}   - V2G discharge
    {"command": "auto"}                               - return to autonomous behavior

State message fields:
    power_kw:             net signed power (+ charging, - discharging)
    charge_kw:            power consumed from grid (0 when discharging)
    discharge_kw:         negative when exporting to grid, 0 otherwise
    state_of_charge:      0.0 to 1.0
    state_of_charge_pct:  0 to 100
    dispatchable_kw:      max V2G power available above driver_min_soc
    driver_min_soc:       minimum SoC driver requires at departure
    departure_time:       ISO8601 UTC datetime when driver needs the car
    plugged_in:           1 if connected, 0 if away
    safe_to_pause:        1 if coordinator can safely pause charging
    mode:                 "charging", "discharging", "paused", "idle", "away"
    dispatch_active:      1 if responding to coordinator signal

Real-world context:
    Smart/managed EV charging is one of the most cost-effective demand
    response resources available to VPP operators. As EV adoption grows,
    coordinated charging fleets become a significant grid flexibility asset.
    V2G is still emerging commercially but is active in several markets
    including the UK (Octopus Energy) and parts of the US and Japan.

    Charge rates by connector type (for reference):
        Level 1 (120V):   1.4 kW   — standard outlet
        Level 2 (240V):   7.2 kW   — home charger, most common for VPP
        DC Fast Charge:   50-350kW — not typically used for V2G
"""

import random
from datetime import datetime, timezone, timedelta
from simulator.asset_base import AssetBase
from grid.sim_clock import SimClock



class EvChargerAsset(AssetBase):
    """
    Simulates a bidirectional EV charger with V2G capability.

    Models autonomous charging behavior (charges when plugged in and
    below target SoC) plus coordinator dispatch for smart charging
    and V2G discharge.

    The coordinator should use dispatchable_kw and safe_to_pause from
    the state message rather than computing them externally — same
    philosophy as BessAsset.

    Args:
        asset_id:           Unique identifier, e.g. "ev-01"
        charge_rate_kw:     Max charge/discharge rate (default 7.2kW = Level 2)
        battery_kwh:        Vehicle battery capacity in kWh
        initial_soc:        Starting state of charge 0.0-1.0 (default 0.3)
        driver_min_soc:     Minimum SoC driver requires at departure (default 0.3)
        plugged_in_hour:    Hour (local Texas time) vehicle arrives home (default 18 = 6pm)
        departure_hour:     Hour (local Texas time) vehicle departs (default 8 = 8am)
    """

    # Battery protection floor — below this the V2G inverter will not operate
    # This is separate from driver_min_soc which is a human preference
    SOC_FLOOR = 0.05

    # Target SoC for autonomous charging — charges to this when plugged in
    # without a coordinator command
    SOC_TARGET = 0.90

    def __init__(
        self,
        asset_id: str,
        charge_rate_kw: float = 7.2,
        battery_kwh: float = 60.0,
        initial_soc: float = 0.3,
        driver_min_soc: float = 0.3,
        plugged_in_hour: int = 18,
        departure_hour: int = 8,
    ):
        super().__init__(asset_id=asset_id, asset_type="ev_charger")

        self.charge_rate_kw = charge_rate_kw
        self.battery_kwh = battery_kwh
        self.soc = initial_soc
        self.driver_min_soc = driver_min_soc
        self.plugged_in_hour = plugged_in_hour
        self.departure_hour = departure_hour

        self._mode = "idle"
        self._target_kw = 0.0
        self._current_power_kw = 0.0
        self._dispatch_active = False

        self.logger.info(
            f"EV charger initialized: {charge_rate_kw}kW / {battery_kwh}kWh "
            f"| initial SoC: {initial_soc:.0%} "
            f"| driver min: {driver_min_soc:.0%} "
            f"| plugs in: {plugged_in_hour:02d}:00 "
            f"| departs: {departure_hour:02d}:00 Texas time"
        )

    # ------------------------------------------------------------------
    # Time and schedule helpers
    # ------------------------------------------------------------------

    def _texas_hour(self) -> float:
        """Returns current hour in Texas local time (CDT = UTC-5)."""
        return SimClock.texas_hour()

    def _is_plugged_in(self) -> bool:
        """
        Returns True if the vehicle should currently be plugged in
        based on the simulated schedule.

        Plugged in from plugged_in_hour until departure_hour the next morning.
        Handles overnight window (e.g. 18:00 to 08:00).
        """
        # CLEAN THIS UP!
        hour = SimClock.texas_hour()

        if self.plugged_in_hour > self.departure_hour:
            # Overnight window — e.g. plugged in at 18, departs at 8
            return hour >= self.plugged_in_hour or hour < self.departure_hour
        else:
            # Same-day window (unusual but handled)
            return self.plugged_in_hour <= hour < self.departure_hour

    def _get_departure_time(self) -> datetime:
        """
        Returns the next departure time as a UTC datetime.
        Used to compute safe_to_pause and published in state message.
        """
        now = SimClock.now()
        texas_hour = SimClock.texas_hour()

        # Calculate hours until next departure
        hours_until_departure = (self.departure_hour - texas_hour) % 24
        if hours_until_departure == 0:
            hours_until_departure = 24

        return now + timedelta(hours=hours_until_departure)

    def _get_safe_to_pause(self) -> bool:
        """
        Returns True if the coordinator can safely pause charging right now.

        Safe to pause means there is enough time remaining before departure
        to charge from current SoC to driver_min_soc at full charge rate.

        Calculation:
            energy_needed_kwh = (driver_min_soc - soc) * battery_kwh
            time_needed_hours = energy_needed_kwh / charge_rate_kw
            time_available_hours = (departure_time - now).seconds / 3600

        If already at or above driver_min_soc, always safe to pause.
        If not plugged in, not relevant — returns False.
        """
        if not self._is_plugged_in():
            return False

        if self.soc >= self.driver_min_soc:
            return True

        energy_needed = (self.driver_min_soc - self.soc) * self.battery_kwh
        time_needed_hours = energy_needed / self.charge_rate_kw

        departure = self._get_departure_time()
        now = SimClock.now()
        time_available_hours = (departure - now).total_seconds() / 3600.0

        # Add a 10% buffer so we don't cut it too close
        return time_available_hours > (time_needed_hours * 1.10)

    def _get_dispatchable_kw(self) -> float:
        """
        Returns maximum V2G discharge power available right now.

        Only available above driver_min_soc (not SOC_FLOOR).
        Returns 0.0 if not plugged in, below driver minimum, or
        currently charging autonomously below target.

        The coordinator uses this field — it never needs to know
        driver_min_soc or battery_kwh directly.
        """
        if not self._is_plugged_in():
            return 0.0

        if self.soc <= self.driver_min_soc:
            return 0.0

        # Energy available above driver minimum
        energy_headroom = (self.soc - self.driver_min_soc) * self.battery_kwh
        interval_hours = self.PUBLISH_INTERVAL_SIM_SEC / 3600.0
        max_from_soc = energy_headroom / interval_hours

        return round(min(self.charge_rate_kw, max_from_soc), 2)

    # ------------------------------------------------------------------
    # Physics simulation
    # ------------------------------------------------------------------

    def _update_soc(self) -> None:
        """
        Updates SoC based on current power flow and elapsed time.

        Positive power_kw = charging = SoC increases
        Negative power_kw = discharging = SoC decreases

        Stops autonomous charging at SOC_TARGET.
        Stops discharge at driver_min_soc (not SOC_FLOOR) to protect
        driver range — the inverter hardware floor SOC_FLOOR is a
        last-resort protection only.
        """
        interval_hours = self.PUBLISH_INTERVAL_SIM_SEC / 3600.0
        delta_soc = (self._current_power_kw * interval_hours) / self.battery_kwh
        new_soc = self.soc + delta_soc

        if new_soc >= self.SOC_TARGET and self._mode == "charging":
            self.soc = self.SOC_TARGET
            self.logger.info("SOC target reached — stopping autonomous charge")
            self._mode = "idle"
            self._target_kw = 0.0
            self._dispatch_active = False

        elif new_soc <= self.driver_min_soc and self._mode == "discharging":
            self.soc = self.driver_min_soc
            self.logger.info("Driver minimum SoC reached — stopping V2G discharge")
            self._mode = "idle"
            self._target_kw = 0.0
            self._dispatch_active = False

        elif new_soc <= self.SOC_FLOOR:
            # Hardware protection floor — should rarely be hit
            self.soc = self.SOC_FLOOR
            self._mode = "idle"
            self._target_kw = 0.0
            self._dispatch_active = False
            self.logger.warning("Hardware SoC floor reached — emergency stop")

        else:
            self.soc = new_soc

    def _compute_power(self) -> float:
        """
        Computes actual power for this interval.

        Autonomous charging: if plugged in, below SOC_TARGET, not under
        dispatch, and no coordinator override — charge at full rate.

        Sign convention:
            positive = charging (consuming from grid)
            negative = discharging (exporting to grid)
        """
        if not self._is_plugged_in():
            self._mode = "away"
            return 0.0

        # Autonomous charging behavior — no coordinator needed
        # Kicks in when plugged in and below target, unless coordinator
        # has issued a stop_charge or discharge command
        if not self._dispatch_active and self.soc < self.SOC_TARGET:
            self._mode = "charging"
            noise = self.charge_rate_kw * random.uniform(-0.01, 0.01)
            return self.charge_rate_kw + noise

        if self._mode == "charging":
            noise = self.charge_rate_kw * random.uniform(-0.01, 0.01)
            return min(abs(self._target_kw), self.charge_rate_kw) + noise

        if self._mode == "discharging":
            noise = self.charge_rate_kw * random.uniform(-0.01, 0.01)
            actual = min(abs(self._target_kw), self.charge_rate_kw) + noise
            return -actual

        # paused, idle
        return 0.0

    # ------------------------------------------------------------------
    # AssetBase interface implementation
    # ------------------------------------------------------------------

    def get_state(self) -> dict:
        """
        Returns current EV charger state. Called every publish interval.

        Splits power into charge_kw (always >= 0) and discharge_kw
        (always <= 0) for Grafana stacking, same convention as BessAsset.
        """
        # At the top of get_state(), before computing anything:
        if not self._is_plugged_in():
            return {
                "power_kw":             0.0,
                "charge_kw":            0.0,
                "discharge_kw":         0.0,
                "state_of_charge":      None,
                "state_of_charge_pct":  None,
                "dispatchable_kw":      0.0,
                "driver_min_soc":       self.driver_min_soc,
                "departure_time":       self._get_departure_time().isoformat(),
                "plugged_in":           0,
                "safe_to_pause":        0,
                "battery_kwh":          self.battery_kwh,
                "charge_rate_kw":       self.charge_rate_kw,
                "mode":                 "away",
                "dispatch_active":      0,
            }
        self._current_power_kw = self._compute_power()
        self._update_soc()

        charge_kw = round(self._current_power_kw, 2) if self._current_power_kw > 0 else 0.0
        discharge_kw = round(self._current_power_kw, 2) if self._current_power_kw < 0 else 0.0

        return {
            "power_kw":             round(self._current_power_kw, 2),
            "charge_kw":            charge_kw,
            "discharge_kw":         discharge_kw,
            "state_of_charge":      round(self.soc, 4),
            "state_of_charge_pct":  round(self.soc * 100, 1),
            "dispatchable_kw":      self._get_dispatchable_kw(),
            "driver_min_soc":       self.driver_min_soc,
            "departure_time":       self._get_departure_time().isoformat(),
            "plugged_in":           1 if self._is_plugged_in() else 0,
            "safe_to_pause":        1 if self._get_safe_to_pause() else 0,
            "battery_kwh":          self.battery_kwh,
            "charge_rate_kw":       self.charge_rate_kw,
            "mode":                 self._mode,
            "dispatch_active":      1 if self._dispatch_active else 0,
        }

    def on_dispatch(self, signal: dict) -> None:
        """
        Responds to coordinator dispatch signals.

        Accepted commands:
            {"command": "charge",       "target_kw": 7.2}
            {"command": "stop_charge"}
            {"command": "discharge",    "target_kw": 7.2}
            {"command": "auto"}          - return to autonomous behavior

        The coordinator should check safe_to_pause before issuing
        stop_charge, and dispatchable_kw before issuing discharge.
        """
        command = signal.get("command")

        if command == "discharge":
            if not self._is_plugged_in():
                self.logger.warning("Discharge command ignored — vehicle not plugged in")
                return
            target = float(signal.get("target_kw", self.charge_rate_kw))
            self._mode = "discharging"
            self._target_kw = target
            self._dispatch_active = True
            self.logger.info(
                f"Dispatched: V2G discharging at {target}kW | SoC: {self.soc:.0%}"
            )

        elif command == "stop_charge":
            if not self._is_plugged_in():
                self.logger.warning("Stop charge ignored — vehicle not plugged in")
                return
            self._mode = "paused"
            self._target_kw = 0.0
            self._dispatch_active = True
            self.logger.info(
                f"Dispatched: charging paused (load shed) | SoC: {self.soc:.0%}"
            )

        elif command == "charge":
            target = float(signal.get("target_kw", self.charge_rate_kw))
            self._mode = "charging"
            self._target_kw = target
            self._dispatch_active = True
            self.logger.info(
                f"Dispatched: charging at {target}kW | SoC: {self.soc:.0%}"
            )

        elif command == "auto":
            # Return to autonomous behavior — coordinator releases control
            self._mode = "idle"
            self._target_kw = 0.0
            self._dispatch_active = False
            self.logger.info("Dispatched: returning to autonomous charging")

        else:
            self.logger.warning(f"Unknown command: {command}")