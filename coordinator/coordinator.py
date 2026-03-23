"""
coordinator/coordinator.py

VPP Coordinator — aggregates asset state from MQTT, computes net grid load,
and dispatches signals to assets when load exceeds thresholds.

Dispatch priority (lowest disruption to highest):
    1. BESS discharge         — fast, no customer impact
    2. EV V2G discharge       — fast, minimal customer impact (above driver min)
    3. EV stop_charge         — pause charging if safe (no range impact)
    4. Thermostat curtailment — gradual, small comfort impact
    5. Industrial islanding   — disruptive, last resort

Release logic uses hysteresis — dispatch triggers at DISPATCH_THRESHOLD,
release only when load drops to RELEASE_THRESHOLD. This prevents rapid
on/off cycling (thrashing).

Thermostat rebound protection:
    After releasing thermostat curtailment, assets enter a COOLDOWN state
    for THERMOSTAT_COOLDOWN_SEC. During cooldown the coordinator ignores
    the thermostat's load when computing whether further dispatch is needed,
    and will not re-curtail the asset. This prevents the rebound spike from
    triggering another curtailment event and creating an oscillation loop.
"""

import json
import time
import logging
import threading
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional
from grid.sim_clock import SimClock, TIME_SCALE


import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)

# ---------------------------------------------------------------------------
# Dispatch configuration
# ---------------------------------------------------------------------------

# Load thresholds as fraction of PEAK_MW from baseline.py (65.0 MW)
PEAK_MW = 65.0
DISPATCH_THRESHOLD_MW = PEAK_MW * 0.92   # start dispatching at 85% of peak
RELEASE_THRESHOLD_MW  = PEAK_MW * 0.85   # release when load drops to 75%
# Load threshold below which BESS should charge autonomously
# Set well below dispatch threshold so charging only happens
# during genuine overnight troughs
BESS_CHARGE_THRESHOLD_MW = 45.0 
# Target SoC for BESS before evening peak — coordinator charges
# BESS up to this level during off-peak hours
BESS_SOC_TARGET = 0.90

# How long each resource stays dispatched before coordinator auto-releases
# These represent compressed simulation time

# BESS stays dispatched for 30 sim-minutes before auto-release
# (self-managing via SoC anyway, this is just a safety ceiling)
BESS_MAX_DISPATCH_SEC      = (30 * 60) / TIME_SCALE

# EV V2G dispatched for 20 sim-minutes
EV_V2G_MAX_DISPATCH_SEC    = (20 * 60) / TIME_SCALE

# EV charge pause for 15 sim-minutes (short — don't inconvenience driver)
EV_PAUSE_MAX_DISPATCH_SEC  = (15 * 60) / TIME_SCALE

# Thermostat curtailed for minimum 30 sim-minutes before release allowed
THERMOSTAT_MIN_CURTAIL_SEC = (30 * 60) / TIME_SCALE

# Thermostat cooldown 45 sim-minutes after release (rebound window)
THERMOSTAT_COOLDOWN_SEC    = (45 * 60) / TIME_SCALE

INDUSTRIAL_MAX_DISPATCH_SEC   = (60 * 60) / TIME_SCALE   # matches asset min island duration

# How often the coordinator runs its dispatch evaluation loop
COORDINATOR_LOOP_SEC = (5 * 60) / TIME_SCALE


# ---------------------------------------------------------------------------
# Asset state tracking
# ---------------------------------------------------------------------------

class DispatchState(Enum):
    NORMAL     = "normal"
    DISPATCHED = "dispatched"
    CHARGING   = "charging"    # coordinator-directed charge, not a dispatch
    COOLDOWN   = "cooldown"    # thermostat only


@dataclass
class TrackedAsset:
    """
    Coordinator-side record for a single asset.

    Tracks the latest state received from MQTT plus coordinator-managed
    dispatch state (separate from the asset's own mode field).
    """
    asset_id:       str
    asset_type:     str
    last_state:     dict = field(default_factory=dict)
    last_seen:      Optional[float] = None      # monotonic time of last message

    dispatch_state: DispatchState = DispatchState.NORMAL
    dispatched_at:  Optional[float] = None      # monotonic time of dispatch
    cooldown_until: Optional[float] = None      # monotonic time cooldown ends

    def is_stale(self, max_age_sec: float = 60.0) -> bool:
        """Returns True if we haven't heard from this asset recently."""
        if self.last_seen is None:
            return True
        return (time.monotonic() - self.last_seen) > max_age_sec

    def time_dispatched_sec(self) -> float:
        """Returns how long this asset has been in dispatched state."""
        if self.dispatched_at is None:
            return 0.0
        return time.monotonic() - self.dispatched_at

    def in_cooldown(self) -> bool:
        """Returns True if this asset is in thermostat cooldown period."""
        if self.cooldown_until is None:
            return False
        return time.monotonic() < self.cooldown_until


# ---------------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------------

class Coordinator:
    """
    VPP Coordinator — the brain of the simulation.

    Subscribes to all asset state topics, maintains a registry of known
    assets, computes aggregate grid load, and dispatches signals when
    load crosses thresholds.

    Architecture:
        - MQTT loop runs in background thread (handles incoming state messages)
        - Dispatch loop runs in main thread (evaluates and acts every N seconds)
        - Asset registry is shared state — access is protected by a lock

    Args:
        broker: MQTT broker hostname
        port:   MQTT broker port
    """

    def __init__(self, broker: str, port: int):
        self.broker = broker
        self.port = port
        self.logger = logging.getLogger("coordinator")

        # Asset registry — keyed by asset_id
        self._assets: dict[str, TrackedAsset] = {}
        self._registry_lock = threading.Lock()

        # Current baseline grid load (updated each loop iteration)
        self._baseline_load_mw = 0.0

        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    # ------------------------------------------------------------------
    # MQTT handlers
    # ------------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            # Subscribe to all asset state topics
            client.subscribe("vpp/assets/+/state")
            # Also subscribe to grid load topic published by baseline
            client.subscribe("vpp/grid/load")
            self.logger.info("Connected — subscribed to vpp/assets/+/state and vpp/grid/load")
        else:
            self.logger.error(f"Connection failed: {rc}")

    def _on_message(self, client, userdata, msg):
        """
        Handles incoming MQTT messages.
        Updates asset registry with latest state.
        Thread-safe via registry lock.
        """
        try:
            data = json.loads(msg.payload.decode())
            topic = msg.topic

            if topic == "vpp/grid/load":
                self._baseline_load_mw = float(data.get("load_mw", 0.0))
                return

            # Extract asset identity from topic: vpp/assets/{asset_id}/state
            parts = topic.split("/")
            if len(parts) != 4:
                return

            asset_id = parts[2]
            asset_type = data.get("asset_type", "unknown")

            with self._registry_lock:
                if asset_id not in self._assets:
                    self.logger.info(
                        f"New asset registered: {asset_id} ({asset_type})"
                    )
                    self._assets[asset_id] = TrackedAsset(
                        asset_id=asset_id,
                        asset_type=asset_type,
                    )

                self._assets[asset_id].last_state = data
                self._assets[asset_id].last_seen = time.monotonic()

        except (json.JSONDecodeError, KeyError) as e:
            self.logger.error(f"Error processing message on {msg.topic}: {e}")

    # ------------------------------------------------------------------
    # Dispatch signal publishing
    # ------------------------------------------------------------------

    def _publish_dispatch(self, asset_id: str, signal: dict) -> None:
        """Publishes a dispatch signal to a specific asset."""
        topic = f"vpp/assets/{asset_id}/dispatch"
        payload = json.dumps(signal)
        self._client.publish(topic, payload)
        self.logger.info(f"Dispatched to {asset_id}: {signal}")

    # ------------------------------------------------------------------
    # Fleet status publishing
    # ------------------------------------------------------------------

    def _write_fleet_status(self) -> None:
        """
        Writes coordinator-side fleet status to InfluxDB every loop.
        This is separate from asset telemetry — it captures the coordinator's
        view of each asset's dispatch state which is not available from
        asset messages alone (e.g. cooldown state only exists in coordinator).
        """
        from influxdb_client import InfluxDBClient, Point
        from influxdb_client.client.write_api import SYNCHRONOUS
        import os

        if not hasattr(self, '_influx_write_api'):
            client = InfluxDBClient(
                url=os.getenv("INFLUXDB_URL"),
                token=os.getenv("INFLUXDB_TOKEN"),
                org=os.getenv("INFLUXDB_ORG"),
            )
            self._influx_write_api = client.write_api(write_options=SYNCHRONOUS)

        with self._registry_lock:
            assets = list(self._assets.values())

        for asset in assets:
            if asset.is_stale():
                continue

            # Determine if asset is physically unavailable
            # (separate from coordinator dispatch state)
            state = asset.last_state
            unavailable = False
            if asset.asset_type == "ev_charger":
                unavailable = not bool(state.get("plugged_in", 0))
            elif asset.asset_type == "industrial_load":
                unavailable = not bool(state.get("islanding_available", 1)) and \
                          asset.dispatch_state == DispatchState.NORMAL

            point = (
                Point("coordinator_state")
                .tag("asset_id", asset.asset_id)
                .tag("asset_type", asset.asset_type)
                .tag("dispatch_state", asset.dispatch_state.value)
                .field("is_dispatched", 1 if asset.dispatch_state == DispatchState.DISPATCHED else 0)
                .field("is_charging", 1 if asset.dispatch_state == DispatchState.CHARGING else 0)
                .field("is_cooldown", 1 if asset.dispatch_state == DispatchState.COOLDOWN else 0)
                .field("is_normal", 1 if asset.dispatch_state == DispatchState.NORMAL and not unavailable else 0)
                .field("is_unavailable", 1 if unavailable else 0)
                .time(SimClock.now(), "s")
            )

            try:
                self._influx_write_api.write(
                    bucket=os.getenv("INFLUXDB_BUCKET"),
                    org=os.getenv("INFLUXDB_ORG"),
                    record=point
                )
            except Exception as e:
                self.logger.error(f"Fleet status write error: {e}")

    # ------------------------------------------------------------------
    # Grid load computation
    # ------------------------------------------------------------------

    def _compute_net_load_mw(self) -> tuple[float, float]:
        """
        Computes total net grid load in MW from all tracked assets.

        Returns:
            tuple of (net_load_mw, asset_load_mw) where:
                net_load_mw:   baseline + all asset loads - all discharges
                asset_load_mw: asset contribution only (for logging)

        Thermostats in cooldown are excluded from the calculation to
        prevent their rebound load from triggering re-dispatch.

        Asset power is in kW — divide by 1000 to convert to MW.
        """
        asset_load_mw = 0.0

        with self._registry_lock:
            for asset in self._assets.values():
                if asset.is_stale():
                    continue

                state = asset.last_state
                power_kw = float(state.get("power_kw", 0.0))

                # Exclude thermostats in cooldown from load calculation
                # This prevents the rebound spike from triggering re-dispatch
                if (asset.asset_type == "thermostat" and
                        asset.dispatch_state == DispatchState.COOLDOWN and
                        asset.in_cooldown()):
                    self.logger.debug(
                        f"{asset.asset_id} in cooldown — excluded from load calc"
                    )
                    continue

                asset_load_mw += power_kw / 1000.0

        net_load_mw = self._baseline_load_mw + asset_load_mw
        return round(net_load_mw, 4), round(asset_load_mw, 4)

    # ------------------------------------------------------------------
    # Dispatch logic — priority ordered
    # ------------------------------------------------------------------

    def _get_assets_by_type(self, asset_type: str) -> list[TrackedAsset]:
        """Returns non-stale assets of a given type, sorted by asset_id."""
        with self._registry_lock:
            return sorted(
                [a for a in self._assets.values()
                 if a.asset_type == asset_type and not a.is_stale()],
                key=lambda a: a.asset_id
            )

    def _dispatch_bess(self) -> bool:
        """
        Priority 1: Dispatch BESS assets to discharge.
        Returns True if any asset was dispatched.
        """
        dispatched = False
        for asset in self._get_assets_by_type("bess"):
            if asset.dispatch_state not in (DispatchState.NORMAL, DispatchState.CHARGING):
                continue

            state = asset.last_state
            dispatchable_kw = float(state.get("dispatchable_kw", 0.0))

            if dispatchable_kw <= 0:
                self.logger.info(
                    f"{asset.asset_id}: no dispatchable capacity available"
                )
                continue

            self._publish_dispatch(
                asset.asset_id,
                {"command": "discharge", "target_kw": dispatchable_kw}
            )
            asset.dispatch_state = DispatchState.DISPATCHED
            asset.dispatched_at = time.monotonic()
            dispatched = True

        return dispatched

    def _dispatch_ev_v2g(self) -> bool:
        """
        Priority 2: Dispatch EV assets to V2G discharge.
        Returns True if any asset was dispatched.
        """
        dispatched = False
        for asset in self._get_assets_by_type("ev_charger"):
            if asset.dispatch_state != DispatchState.NORMAL:
                continue

            state = asset.last_state
            if not state.get("plugged_in"):
                continue

            dispatchable_kw = float(state.get("dispatchable_kw", 0.0))
            if dispatchable_kw <= 0:
                continue

            self._publish_dispatch(
                asset.asset_id,
                {"command": "discharge", "target_kw": dispatchable_kw}
            )
            asset.dispatch_state = DispatchState.DISPATCHED
            asset.dispatched_at = time.monotonic()
            dispatched = True

        return dispatched

    def _dispatch_ev_pause(self) -> bool:
        """
        Priority 3: Pause EV charging (load shed without V2G).
        Only safe if safe_to_pause is True.
        Returns True if any asset was dispatched.
        """
        dispatched = False
        for asset in self._get_assets_by_type("ev_charger"):
            if asset.dispatch_state != DispatchState.NORMAL:
                continue

            state = asset.last_state
            if not state.get("plugged_in"):
                continue
            if not state.get("safe_to_pause"):
                continue
            # Skip if already not charging (no load to shed)
            if float(state.get("charge_kw", 0.0)) <= 0:
                continue

            self._publish_dispatch(asset.asset_id, {"command": "stop_charge"})
            asset.dispatch_state = DispatchState.DISPATCHED
            asset.dispatched_at = time.monotonic()
            dispatched = True

        return dispatched

    def _dispatch_thermostats(self) -> bool:
        """
        Priority 4: Curtail thermostat setpoints.
        Skips assets in cooldown (rebound protection).
        Returns True if any asset was dispatched.
        """
        dispatched = False
        for asset in self._get_assets_by_type("thermostat"):
            # Skip if dispatched or in cooldown
            if asset.dispatch_state != DispatchState.NORMAL:
                continue
            if asset.in_cooldown():
                continue

            self._publish_dispatch(
                asset.asset_id,
                {"command": "curtail", "setpoint_offset_f": 4.0}
            )
            asset.dispatch_state = DispatchState.DISPATCHED
            asset.dispatched_at = time.monotonic()
            dispatched = True

        return dispatched

    def _dispatch_industrial(self) -> bool:
        """
        Priority 5: Island industrial loads. Last resort.
        Returns True if any asset was dispatched.
        """
        dispatched = False
        for asset in self._get_assets_by_type("industrial_load"):
            if asset.dispatch_state != DispatchState.NORMAL:
                continue

            state = asset.last_state
            if not state.get("islanding_available"):
                continue

            self._publish_dispatch(asset.asset_id, {"command": "island"})
            asset.dispatch_state = DispatchState.DISPATCHED
            asset.dispatched_at = time.monotonic()
            dispatched = True

        return dispatched

    # ------------------------------------------------------------------
    # Release logic
    # ------------------------------------------------------------------

    def _release_assets(self) -> None:
        """
        Releases dispatched assets when load has dropped below release
        threshold or max dispatch duration has been exceeded.

        Thermostat release enters COOLDOWN state rather than NORMAL
        to protect against rebound-triggered re-dispatch.
        """
        with self._registry_lock:
            assets = list(self._assets.values())

        for asset in assets:
            if asset.dispatch_state != DispatchState.DISPATCHED:
                continue

            duration = asset.time_dispatched_sec()
            should_release = False

            if asset.asset_type == "bess":
                # BESS self-manages via SoC — release after max duration
                # or if it has already gone idle on its own
                mode = asset.last_state.get("mode", "")
                if duration > BESS_MAX_DISPATCH_SEC or mode == "idle":
                    should_release = True

            elif asset.asset_type == "ev_charger":
                mode = asset.last_state.get("mode", "")
                max_dur = (EV_V2G_MAX_DISPATCH_SEC
                           if mode == "discharging"
                           else EV_PAUSE_MAX_DISPATCH_SEC)
                if duration > max_dur:
                    should_release = True

            elif asset.asset_type == "thermostat":
                if duration > THERMOSTAT_MIN_CURTAIL_SEC:
                    should_release = True

            elif asset.asset_type == "industrial_load":
                if duration > INDUSTRIAL_MAX_DISPATCH_SEC:
                    should_release = True

            if should_release:
                self._release_asset(asset)

    def _release_asset(self, asset: TrackedAsset) -> None:
        """
        Sends the appropriate release command for an asset type
        and updates its dispatch state.
        """
        if asset.asset_type == "bess":
            self._publish_dispatch(asset.asset_id, {"command": "idle"})
            asset.dispatch_state = DispatchState.NORMAL
            asset.dispatched_at = None

        elif asset.asset_type == "ev_charger":
            self._publish_dispatch(asset.asset_id, {"command": "auto"})
            asset.dispatch_state = DispatchState.NORMAL
            asset.dispatched_at = None

        elif asset.asset_type == "thermostat":
            self._publish_dispatch(asset.asset_id, {"command": "normal"})
            # Enter cooldown instead of normal — rebound protection
            asset.dispatch_state = DispatchState.COOLDOWN
            asset.dispatched_at = None
            asset.cooldown_until = time.monotonic() + THERMOSTAT_COOLDOWN_SEC
            self.logger.info(
                f"{asset.asset_id}: released into cooldown for "
                f"{THERMOSTAT_COOLDOWN_SEC}s"
            )

        elif asset.asset_type == "industrial_load":
            reconnect_time = float(
                asset.last_state.get("time_until_reconnect_sec", 999)
            )
            if reconnect_time <= 0:
                self._publish_dispatch(asset.asset_id, {"command": "reconnect"})
                asset.dispatch_state = DispatchState.NORMAL
                asset.dispatched_at = None
            else:
                self.logger.info(
                    f"{asset.asset_id}: wants to reconnect but "
                    f"{reconnect_time:.0f}s remaining"
                )

    # ------------------------------------------------------------------
    # BESS charging
    # ------------------------------------------------------------------

    def _manage_bess_charging(self, net_load_mw: float) -> None:
        """
        Sends charge commands to BESS assets during off-peak hours
        and idle commands when load rises.

        This keeps BESS assets topped up before evening peak events
        without the asset needing to know anything about grid state.

        Called every loop iteration regardless of dispatch threshold status.
        """
        for asset in self._get_assets_by_type("bess"):
            state = asset.last_state
            soc = float(state.get("state_of_charge", 0.0))
            mode = state.get("mode", "idle")

            # Don't interfere if asset is in a real dispatch event
            if asset.dispatch_state == DispatchState.DISPATCHED:
                continue

            if net_load_mw < BESS_CHARGE_THRESHOLD_MW and soc < BESS_SOC_TARGET:
                if asset.dispatch_state != DispatchState.CHARGING:
                    self._publish_dispatch(
                        asset.asset_id,
                        {"command": "charge",
                        "target_kw": float(state.get("power_rating_kw", 200.0)) * 0.5}
                    )
                    asset.dispatch_state = DispatchState.CHARGING
                    self.logger.info(
                        f"{asset.asset_id}: charging started "
                        f"| SoC: {soc:.0%} | load: {net_load_mw:.1f} MW"
                    )

            elif (net_load_mw >= BESS_CHARGE_THRESHOLD_MW and
                asset.dispatch_state == DispatchState.CHARGING):
                self._publish_dispatch(asset.asset_id, {"command": "idle"})
                asset.dispatch_state = DispatchState.NORMAL
                self.logger.info(
                    f"{asset.asset_id}: charge stopped — load rising: {net_load_mw:.1f} MW"
                )

            elif soc >= BESS_SOC_TARGET and asset.dispatch_state == DispatchState.CHARGING:
                self._publish_dispatch(asset.asset_id, {"command": "idle"})
                asset.dispatch_state = DispatchState.NORMAL
                self.logger.info(
                    f"{asset.asset_id}: charge complete | SoC: {soc:.0%}"
                )



    # ------------------------------------------------------------------
    # Main dispatch evaluation loop
    # ------------------------------------------------------------------

    def _dispatch_loop(self) -> None:
        """
        Main coordinator loop. Runs every COORDINATOR_LOOP_SEC seconds.

        Each iteration:
        1. Compute net grid load
        2. Check if above dispatch threshold → dispatch next priority tier
        3. Check if below release threshold → release dispatched assets
        4. Log current system state
        """
        self.logger.info(
            f"Dispatch loop started | "
            f"dispatch threshold: {DISPATCH_THRESHOLD_MW:.1f} MW | "
            f"release threshold: {RELEASE_THRESHOLD_MW:.1f} MW"
        )

        while True:
            try:
                net_load_mw, asset_load_mw = self._compute_net_load_mw()

                # Count assets in each state for logging
                with self._registry_lock:
                    total = len(self._assets)
                    dispatched = sum(
                        1 for a in self._assets.values()
                        if a.dispatch_state == DispatchState.DISPATCHED
                    )
                    cooldown = sum(
                        1 for a in self._assets.values()
                        if a.dispatch_state == DispatchState.COOLDOWN
                    )

                self.logger.info(
                    f"Net load: {net_load_mw:.2f} MW "
                    f"(baseline: {self._baseline_load_mw:.2f} MW + "
                    f"assets: {asset_load_mw*1000:.1f} kW) | "
                    f"assets: {total} total / {dispatched} dispatched / "
                    f"{cooldown} cooldown"
                )

                # Always manage BESS charging regardless of dispatch state
                self._manage_bess_charging(net_load_mw)

                if net_load_mw >= DISPATCH_THRESHOLD_MW:
                    self.logger.warning(
                        f"DISPATCH EVENT: {net_load_mw:.2f} MW >= "
                        f"{DISPATCH_THRESHOLD_MW:.1f} MW threshold"
                    )
                    # Dispatch one tier per loop iteration — check if anything
                    # is already dispatched at each tier before escalating
                    # This gives each tier time to take effect before calling
                    # on more disruptive resources
                    
                    bess_dispatched = any(
                        a.dispatch_state == DispatchState.DISPATCHED
                        for a in self._assets.values()
                        if a.asset_type == "bess"
                    )
                    ev_dispatched = any(
                        a.dispatch_state == DispatchState.DISPATCHED
                        for a in self._assets.values()
                        if a.asset_type == "ev_charger"
                    )
                    thermostat_dispatched = any(
                        a.dispatch_state == DispatchState.DISPATCHED
                        for a in self._assets.values()
                        if a.asset_type == "thermostat"
                    )
                    industrial_dispatched = any(
                        a.dispatch_state == DispatchState.DISPATCHED
                        for a in self._assets.values()
                        if a.asset_type == "industrial_load"
                    )

                    if not bess_dispatched:
                        # Tier 1: try BESS first
                        self._dispatch_bess()
                    elif not ev_dispatched:
                        # Tier 2: BESS already working, try EV V2G
                        if not self._dispatch_ev_v2g():
                            if not self._dispatch_ev_pause():
                                pass  # wait for next loop before escalating further
                    elif not thermostat_dispatched:
                        # Tier 3: EV already working, try thermostats
                        self._dispatch_thermostats()
                    elif not industrial_dispatched:
                        # Tier 4: last resort
                        self._dispatch_industrial()

                elif net_load_mw < RELEASE_THRESHOLD_MW:
                    self._release_assets()

                self._write_fleet_status()
        
            except Exception as e:
                self.logger.error(f"Error in dispatch loop: {e}", exc_info=True)

            time.sleep(COORDINATOR_LOOP_SEC)           

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Connects to MQTT broker and starts the coordinator.
        MQTT loop runs in background thread, dispatch loop runs here.
        Blocks until interrupted.
        """
        self._client.connect(self.broker, self.port)
        self._client.loop_start()

        self.logger.info(
            f"Coordinator started | broker: {self.broker}:{self.port}"
        )

        try:
            self._dispatch_loop()
        except KeyboardInterrupt:
            self.logger.info("Coordinator shutting down")
            self._client.loop_stop()
            self._client.disconnect()