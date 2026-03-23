"""
grid/sim_clock.py

Simulated clock that runs faster than real time.
All components use SimClock.now() instead of datetime.now()
so the entire simulation operates on compressed time.

TIME_SCALE: how many simulation seconds pass per real second
    1    = real time
    60   = 1 sim minute per real second
    360  = 6 sim hours per real minute (full day in ~4 min)
    3600 = 1 sim hour per real second  (full day in 24 sec)

SIM_DATE: fixed calendar date for all simulation runs.
    Hardcoded to a hot Texas summer day so Grafana dashboard
    queries never need updating between runs.
"""

from datetime import datetime, timezone, timedelta
import time

TIME_SCALE = 360

# Fixed simulation date — always August 15, 2024 (hot Texas summer day)
# Grafana queries use 2024-08-15T00:00:00Z to 2024-08-16T23:59:59Z
SIM_DATE = (2024, 8, 15)


class SimClock:
    """
    Singleton simulated clock.

    All components import and call SimClock.now() to get
    the current simulated datetime.
    """

    _real_start: float = None       # real monotonic time at init
    _sim_start: datetime = None     # simulated datetime at init

    @classmethod
    def initialize(cls, start_texas_hour: int = 0) -> None:
        """
        Call once at simulation startup to set the starting time.

        Args:
            start_texas_hour: What Texas hour the simulation starts at (0-23)
                              Default 0 = midnight Texas time
        """
        cls._real_start = time.monotonic()

        # Convert Texas start hour to UTC (CDT = UTC+5 in summer)
        utc_hour = (start_texas_hour + 5) % 24
        cls._sim_start = datetime(
            SIM_DATE[0], SIM_DATE[1], SIM_DATE[2],
            utc_hour, 0, 0,
            tzinfo=timezone.utc
        )

    @classmethod
    def now(cls) -> datetime:
        """
        Returns the current simulated datetime in UTC.
        Raises RuntimeError if SimClock.initialize() hasn't been called.
        """
        if cls._real_start is None:
            raise RuntimeError(
                "SimClock not initialized — call SimClock.initialize() first"
            )

        real_elapsed = time.monotonic() - cls._real_start
        sim_elapsed = timedelta(seconds=real_elapsed * TIME_SCALE)
        return cls._sim_start + sim_elapsed

    @classmethod
    def texas_hour(cls) -> float:
        """Returns current simulated hour in Texas local time (CDT = UTC-5)."""
        dt = cls.now()
        return (dt.hour - 5) % 24 + dt.minute / 60.0

    @classmethod
    def sim_time_str(cls) -> str:
        """Returns human readable simulated Texas time for display."""
        hour = cls.texas_hour()
        h = int(hour)
        m = int((hour % 1) * 60)
        period = "AM" if h < 12 else "PM"
        h12 = h % 12 or 12
        return f"{h12}:{m:02d} {period} CT (sim)"