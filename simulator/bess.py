"""
simulator/bess.py

Battery Energy Storage System (BESS) asset simulator.

Models a grid-connected battery that can:
  - Charge from the grid (positive power_kw = load on grid)
  - Discharge to the grid (negative power_kw = export to grid)
  - Hold state of charge between 0.0 (empty) and 1.0 (full)
  - Respond to coordinator dispatch signals to charge, discharge, or idle

Power rating (kW):  maximum charge or discharge rate
Energy rating (kWh): total usable capacity

The ratio energy_kwh / power_kw = duration in hours.
A 200kW / 400kWh asset has a 2-hour duration — a common
configuration for grid services like peak shaving and frequency reg.

Dispatch commands accepted:
    {"command": "charge",     "target_kw": 50.0}  - charge at target rate
    {"command": "discharge",  "target_kw": 50.0}  - discharge at target rate
    {"command": "idle"}                            - stop charging/discharging

State message fields:
    power_kw:             net signed power (+ charging, - discharging)
    charge_kw:            power consumed from grid (0 when discharging)
    discharge_kw:         power exported to grid (0 when charging)
    state_of_charge:      0.0 to 1.0
    state_of_charge_pct:  0 to 100 (for Grafana display)
    energy_available_kwh: energy that can still be discharged
    dispatchable_kw:      max power available right now (coordinator uses this)
    power_rating_kw:      nameplate power capacity
    energy_rating_kwh:    nameplate energy capacity
    mode:                 "charge", "discharge", or "idle"
    dispatch_active:      1 if responding to a coordinator signal, 0 if autonomous

Real-world context:
    In ERCOT and other ISOs, BESS assets can participate in:
    - Energy arbitrage (charge cheap, discharge expensive)
    - Ancillary services: frequency regulation (REGUP/REGDN in ERCOT)
    - Emergency response (ERS in ERCOT)
    The coordinator in this simulation approximates the dispatch logic
    an aggregator or VPP operator would use to manage these assets.
"""

import random
from simulator.asset_base import AssetBase


class BessAsset(AssetBase):
    """
    Simulates a grid-connected BESS asset.

    The coordinator should use dispatchable_kw from the state message
    to determine dispatch targets — it does not need to know SOC_MIN,
    SOC_MAX, or any internal BMS parameters. The asset self-reports
    its available capacity and enforces its own limits internally.
    This mirrors real VPP architecture where the BMS on the asset
    enforces hard limits and the coordinator operates at a higher
    abstraction level.

    Args:
        asset_id:          Unique identifier, e.g. "bess-01"
        power_rating_kw:   Max charge/discharge rate in kW
        energy_rating_kwh: Total usable capacity in kWh
        initial_soc:       Starting state of charge, 0.0 to 1.0 (default 0.5)
    """

    # SoC safety limits — real BMS systems protect the battery
    # by never charging to 100% or discharging to 0%
    SOC_MIN = 0.10   # never discharge below 10%
    SOC_MAX = 0.95   # never charge above 95%


    def __init__(
        self,
        asset_id: str,
        power_rating_kw: float,
        energy_rating_kwh: float,
        initial_soc: float = 0.5,
    ):
        super().__init__(asset_id=asset_id, asset_type="bess")

        self.power_rating_kw = power_rating_kw
        self.energy_rating_kwh = energy_rating_kwh
        self.soc = initial_soc

        self._mode = "idle"
        self._target_kw = 0.0
        self._current_power_kw = 0.0
        self._dispatch_active = False

        self.logger.info(
            f"BESS initialized: {power_rating_kw}kW / {energy_rating_kwh}kWh "
            f"| duration: {energy_rating_kwh/power_rating_kw:.1f}h "
            f"| initial SoC: {initial_soc:.0%}"
        )

    # ------------------------------------------------------------------
    # Physics simulation
    # ------------------------------------------------------------------

    def _update_soc(self) -> None:
        """
        Updates state of charge based on current power flow and elapsed time.

            delta_soc = (power_kw * interval_hours) / energy_rating_kwh

        Positive power_kw = charging = SoC increases
        Negative power_kw = discharging = SoC decreases

        Clamps to [SOC_MIN, SOC_MAX] and cuts power if limits are hit,
        mimicking real BMS behavior.
        """
        interval_hours = self.PUBLISH_INTERVAL_SIM_SEC / 3600.0
        delta_soc = (self._current_power_kw * interval_hours) / self.energy_rating_kwh
        new_soc = self.soc + delta_soc

        if new_soc >= self.SOC_MAX:
            self.soc = self.SOC_MAX
            if self._mode in ("charge"):
                self.logger.info("SoC limit reached — stopping charge")
                self._mode = "idle"
                self._target_kw = 0.0
                self._dispatch_active = False
        elif new_soc <= self.SOC_MIN:
            self.soc = self.SOC_MIN
            if self._mode == "discharge":
                self.logger.info("SoC minimum reached — stopping discharge")
                self._mode = "idle"
                self._target_kw = 0.0
                self._dispatch_active = False
        else:
            self.soc = new_soc

    def _compute_power(self) -> float:
        """
        Computes actual net signed power for this interval.

        Sign convention:
            positive = charging (consuming from grid)
            negative = discharging (exporting to grid)

        Clamps to power rating and adds small inverter noise (~1%).

        Returns:
            float — net power in kW
        """
        if self._mode == "idle":
            return 0.0

        clamped = min(abs(self._target_kw), self.power_rating_kw)
        noise = clamped * random.uniform(-0.01, 0.01)
        actual = clamped + noise

        return actual if self._mode == "charge" else -actual

    def _get_dispatchable_kw(self) -> float:
        """
        Returns the maximum power this asset can discharge right now.

        The coordinator uses this field instead of raw SoC values —
        it never needs to know SOC_MIN, SOC_MAX, or energy_rating_kwh.
        The asset self-reports its available capacity and the coordinator
        simply asks "how much can you give me?"

        Accounts for:
            - Current SoC relative to SOC_MIN
            - Power rating ceiling
            - Current interval duration

        Returns 0.0 if asset is at or below minimum SoC.
        """
        if self.soc <= self.SOC_MIN:
            return 0.0

        energy_headroom = (self.soc - self.SOC_MIN) * self.energy_rating_kwh
        interval_hours = self.PUBLISH_INTERVAL_SIM_SEC / 3600.0
        max_from_soc = energy_headroom / interval_hours

        return round(min(self.power_rating_kw, max_from_soc), 2)

    # ------------------------------------------------------------------
    # AssetBase interface implementation
    # ------------------------------------------------------------------

    def get_state(self) -> dict:
        """
        Returns current BESS state. Called every publish interval by base class.

        Splits power into charge_kw and discharge_kw for Grafana stacking:
            - charge_kw is positive when consuming, 0 otherwise
            - discharge_kw is positive when exporting, 0 otherwise
            - power_kw is the net signed value (used for the centered panel)

        The coordinator uses dispatchable_kw to determine dispatch targets
        without needing to know internal BMS parameters.
        """
        self._current_power_kw = self._compute_power()
        self._update_soc()


        # Split into directional fields for Grafana stacking
        # charge_kw:    positive when consuming from grid, 0 otherwise
        # discharge_kw: negative when exporting to grid, 0 otherwise
        # power_kw:     net signed value, equivalent to charge_kw + discharge_kw
        charge_kw = round(self._current_power_kw, 2) if self._current_power_kw > 0 else 0.0
        discharge_kw = round(self._current_power_kw, 2) if self._current_power_kw < 0 else 0.0

        energy_available_kwh = round(
            (self.soc - self.SOC_MIN) * self.energy_rating_kwh, 2
        )

        return {
            "power_kw":             round(self._current_power_kw, 2),
            "charge_kw":            charge_kw,
            "discharge_kw":         discharge_kw,
            "state_of_charge":      round(self.soc, 4),
            "state_of_charge_pct":  round(self.soc * 100, 1),
            "energy_available_kwh": energy_available_kwh,
            "dispatchable_kw":      self._get_dispatchable_kw(),
            "power_rating_kw":      self.power_rating_kw,
            "energy_rating_kwh":    self.energy_rating_kwh,
            "mode":                 self._mode,
            "dispatch_active":      1 if self._dispatch_active else 0,
        }

    def on_dispatch(self, signal: dict) -> None:
        """
        Responds to coordinator dispatch signals.

        Accepted commands:
            {"command": "charge",    "target_kw": 50.0}
            {"command": "discharge", "target_kw": 50.0}
            {"command": "idle"}

        The coordinator should check dispatchable_kw from the state message
        before sending a discharge command rather than computing it externally.
        """
        command = signal.get("command")

        if command == "discharge":
            target = float(signal.get("target_kw", self.power_rating_kw))
            self._mode = "discharge"
            self._target_kw = target
            self._dispatch_active = True
            self.logger.info(
                f"Dispatched: discharging at {target}kW | SoC: {self.soc:.0%}"
            )

        elif command == "charge":
            target = float(signal.get("target_kw", self.power_rating_kw))
            self._mode = "charge"
            self._target_kw = target
            self._dispatch_active = False
            self.logger.info(
                f"Dispatched: charging at {target}kW | SoC: {self.soc:.0%}"
            )

        elif command == "idle":
            self._mode = "idle"
            self._target_kw = 0.0
            self._dispatch_active = False
            self.logger.info("Dispatched: idling")

        else:
            self.logger.warning(f"Unknown command: {command}")