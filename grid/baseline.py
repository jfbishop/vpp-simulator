"""
grid/baseline.py

Generates a synthetic baseline load curve shaped like a typical ERCOT
summer day in Texas. The curve is used by the coordinator to simulate
total grid demand before VPP assets are factored in.

ERCOT DATA SWAP:
    To replace this synthetic curve with real ERCOT data:
    1. Register for ERCOT's API at https://developer.ercot.com/
    2. Use the "Load Forecasts by Weather Zone" or "Actual System Load by
       Weather Zone" endpoints
    3. Pull historical data for your target date range using:
           GET /api/public-reports/act-sys-load-by-wzn
    4. Store the response as a CSV and load it with pandas:
           df = pd.read_csv("ercot_load.csv", parse_dates=["timestamp"])
    5. Replace get_baseline_load() with a lookup against that dataframe
       rather than computing the synthetic curve below
"""

import random
from datetime import datetime, timezone
from grid.sim_clock import SimClock

# ---------------------------------------------------------------------------
# Anchor points defining the load curve shape (Texas local time)
# Each entry is [hour, load_mw]. Values between anchors are linearly
# interpolated. Shape is calibrated to ERCOT summer patterns, scaled down
# to represent a ~50-100 MW VPP service territory.
#
# Key features:
#   - Overnight trough ~25 MW (3-5am)
#   - Morning ramp starting 6am
#   - Midday AC-driven plateau ~55 MW (noon-4pm)
#   - Brief dip ~5-6pm as solar helps offset load
#   - Evening peak ~65 MW at 8pm as solar drops and cooling continues
#   - Sharp decline after 10pm
#
# ERCOT DATA SWAP: replace this table with a pandas dataframe lookup
# ---------------------------------------------------------------------------

LOAD_ANCHORS = [
    (0,  26.0),
    (1,  25.5),
    (2,  25.2),
    (3,  25.0),
    (4,  25.1),
    (5,  26.0),
    (6,  28.5),
    (7,  33.0),
    (8,  38.5),
    (9,  43.0),
    (10, 47.0),
    (11, 51.0),
    (12, 53.5),
    (13, 55.0),
    (14, 55.5),
    (15, 55.0),
    (16, 54.0),
    (17, 55.5),
    (18, 59.0),
    (19, 63.5),
    (20, 65.0),
    (21, 62.0),
    (22, 55.0),
    (23, 43.0),
]

PEAK_MW = 65.0


def _interpolate(hour_float: float) -> float:
    """
    Linearly interpolates load between anchor points for a given hour.

    Args:
        hour_float: Hour as a float 0.0–23.99 in Texas local time

    Returns:
        float — interpolated load in MW
    """
    h = int(hour_float) % 24
    next_h = (h + 1) % 24
    fraction = hour_float - int(hour_float)

    load_now = LOAD_ANCHORS[h][1]
    load_next = LOAD_ANCHORS[next_h][1]

    return load_now + fraction * (load_next - load_now)


def _add_noise(value: float, noise_pct: float = 0.02) -> float:
    """
    Adds small random variation to avoid an unrealistically smooth curve.

    Args:
        value: Base load value in MW
        noise_pct: Max variation as a fraction of value (default 2%)

    Returns:
        float — value with small perturbation applied
    """
    variation = value * noise_pct
    return value + random.uniform(-variation, variation)


def get_baseline_load(dt: datetime = None) -> float:
    """
    Returns simulated baseline grid load in MW for a given datetime.

    Args:
        dt: A timezone-aware datetime. Defaults to current UTC time.

    Returns:
        float — baseline load in MW

    Example:
        >>> from datetime import datetime, timezone
        >>> load = get_baseline_load(datetime(2024, 7, 15, 1, 0, tzinfo=timezone.utc))
        >>> print(f"Evening peak load: {load:.1f} MW")

    ERCOT DATA SWAP:
        Replace the body of this function with a pandas lookup:
        row = df[df["timestamp"] == dt.floor("15min")]
        if not row.empty:
            return float(row["load_mw"].iloc[0])
        raise ValueError(f"No ERCOT data found for timestamp {dt}")
    """
    if dt is None:
        dt = SimClock.now()

    # Convert UTC to Texas summer time (CDT = UTC-5, CST = UTC-6)
    # Using -5 for summer (daylight saving). For a production system
    # use the pytz or zoneinfo library to handle DST automatically:
    #   from zoneinfo import ZoneInfo
    #   texas_dt = dt.astimezone(ZoneInfo("America/Chicago"))
    texas_hour = (dt.hour - 5) % 24
    hour_float = texas_hour + dt.minute / 60.0

    load_mw = _interpolate(hour_float)
    return round(_add_noise(load_mw), 2)


def get_dispatch_threshold(headroom_pct: float = 0.85) -> float:
    """
    Returns the MW level at which the coordinator should begin dispatching
    VPP assets to reduce load or inject storage.

    Set as a percentage of peak load — by default 85% of PEAK_MW, which
    mirrors how real grid operators set high-load alerts before calling
    on demand response resources.

    Args:
        headroom_pct: Fraction of peak at which to trigger dispatch (default 0.85)

    Returns:
        float — dispatch threshold in MW

    ERCOT DATA SWAP:
        Could be derived dynamically from ERCOT's real-time operating
        reserve threshold published via their API.
    """
    return round(PEAK_MW * headroom_pct, 2)