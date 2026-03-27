"""
IoDFT Automatic Pitfall Detector
Analyses a time series and detects data pitfalls automatically
using rule-based methods aligned with IoDFT taxonomy.

Fully automatic: infers sampling rate, bounds, and thresholds
from the data itself. No configuration required.

Usage:
    from iodft.detector import detect_pitfalls
    
    pitfalls = detect_pitfalls(timestamps, values)
"""

import numpy as np
from iodft.taxonomy import DEFAULT_CONFIG


class DetectedPitfall:
    """Represents a single detected pitfall event with location and metadata."""

    def __init__(self, pitfall_type, start_idx, end_idx, severity, detail=""):
        self.pitfall_type = pitfall_type
        self.start_idx = start_idx
        self.end_idx = end_idx
        self.duration_points = end_idx - start_idx
        self.severity = severity
        self.detail = detail

    def __repr__(self):
        return (
            f"DetectedPitfall('{self.pitfall_type}', "
            f"idx=[{self.start_idx}:{self.end_idx}], "
            f"duration={self.duration_points}, "
            f"severity={self.severity:.3f})"
        )


def detect_pitfalls(timestamps, values, expected_interval=None, config=None):
    """
    Analyse a time series and detect IoDFT pitfalls automatically.

    Parameters
    ----------
    timestamps : array-like
        Timestamps as datetime objects, numpy datetime64, or numeric seconds.
    values : array-like
        Sensor readings as numeric values.
    expected_interval : float, optional
        Expected sampling interval in seconds. If None, it is
        automatically inferred from the median inter-reading interval.
    config : dict, optional
        Override default detection parameters. See taxonomy.DEFAULT_CONFIG.

    Returns
    -------
    list of DetectedPitfall
        All detected pitfall events, sorted by start index.
    """
    cfg = DEFAULT_CONFIG.copy()
    if config:
        cfg.update(config)

    values = np.array(values, dtype=float)
    n = len(values)

    if n < 2:
        return []

    # Compute inter-reading intervals in seconds
    intervals = _compute_intervals(timestamps)

    # Auto-detect sampling rate if not provided
    if expected_interval is None:
        expected_interval = _infer_sampling_rate(intervals)
        cfg["_inferred_interval"] = expected_interval

    # Auto-detect value bounds from data
    bounds = _infer_bounds(values, cfg)
    cfg["_lower_bound"] = bounds[0]
    cfg["_upper_bound"] = bounds[1]

    # Auto-detect rolling window from sampling rate
    if cfg["rolling_window_size"] == 60:
        cfg["rolling_window_size"] = _infer_window_size(
            expected_interval, n
        )

    # Run all detectors
    pitfalls = []
    pitfalls.extend(_detect_missing(values, intervals, expected_interval, cfg))
    pitfalls.extend(_detect_stuck_at(values, cfg))
    pitfalls.extend(_detect_spike(values, cfg))
    pitfalls.extend(_detect_out_of_bound(values, bounds, cfg))
    pitfalls.extend(_detect_nonfunctional(values, intervals, expected_interval, cfg))
    pitfalls.extend(_detect_slow_response(intervals, expected_interval, cfg))
    pitfalls.extend(_detect_degradation(values, cfg))

    # Sort by start index
    pitfalls.sort(key=lambda p: p.start_idx)

    return pitfalls


# ============================================================
# AUTO-INFERENCE
# ============================================================

def _infer_sampling_rate(intervals):
    """
    Infer the expected sampling interval from the data.
    Uses the median interval, which is robust to gaps and outliers.
    """
    if len(intervals) == 0:
        return 60.0

    clean = intervals[intervals > 0]
    if len(clean) == 0:
        return 60.0

    median_interval = float(np.median(clean))
    return median_interval


def _infer_bounds(values, cfg):
    """
    Infer plausible value bounds from the data distribution.
    Uses IQR method: [Q1 - k*IQR, Q3 + k*IQR].
    Falls back to percentile method for skewed distributions.
    """
    clean = values[~np.isnan(values)]

    if len(clean) < 10:
        return (float(np.min(clean)), float(np.max(clean)))

    q1 = float(np.percentile(clean, 25))
    q3 = float(np.percentile(clean, 75))
    iqr = q3 - q1

    if iqr < 1e-6:
        # Very narrow distribution — use percentiles instead
        lower = float(np.percentile(clean, 1))
        upper = float(np.percentile(clean, 99))
    else:
        k = cfg["iqr_multiplier"]
        lower = q1 - k * iqr
        upper = q3 + k * iqr

    return (lower, upper)


def _infer_window_size(expected_interval, n):
    """
    Infer an appropriate rolling window size based on sampling rate.
    Targets approximately 1 hour of data, bounded by data length.
    """
    one_hour_points = max(10, int(3600 / max(1, expected_interval)))
    # Window should not exceed 10% of data length
    max_window = max(10, n // 10)
    return min(one_hour_points, max_window)


def _compute_intervals(timestamps):
    """Compute inter-reading intervals in seconds."""
    ts = np.array(timestamps)

    if len(ts) < 2:
        return np.array([])

    # Handle datetime objects
    if hasattr(ts[0], 'timestamp'):
        seconds = np.array([t.timestamp() for t in ts])
        return np.diff(seconds)
    # Handle numpy datetime64
    elif np.issubdtype(ts.dtype, np.datetime64):
        return np.diff(ts).astype("timedelta64[s]").astype(float)
    # Handle pandas Timestamp
    elif hasattr(ts[0], 'value'):
        seconds = np.array([t.value / 1e9 for t in ts])
        return np.diff(seconds)
    # Assume already numeric seconds
    else:
        return np.diff(ts.astype(float))


# ============================================================
# PITFALL DETECTORS
# ============================================================

def _detect_missing(values, intervals, expected_interval, cfg):
    """
    Detect MISSING pitfall.
    Gaps where inter-reading interval exceeds threshold.
    Also detects NaN/null values.
    """
    pitfalls = []
    gap_threshold = expected_interval * cfg["gap_multiplier"]

    # Gap-based missing
    if len(intervals) > 0:
        gap_mask = intervals > gap_threshold
        gap_indices = np.where(gap_mask)[0]

        events = _group_consecutive(gap_indices)
        for start, end in events:
            gap_duration = np.sum(intervals[start:end + 1])
            pitfalls.append(DetectedPitfall(
                pitfall_type="missing",
                start_idx=start,
                end_idx=end + 1,
                severity=gap_duration / gap_threshold,
                detail=f"Gap of {gap_duration:.1f}s "
                       f"(threshold: {gap_threshold:.1f}s, "
                       f"inferred rate: {expected_interval:.1f}s)"
            ))

    # NaN-based missing
    nan_indices = np.where(np.isnan(values))[0]
    if len(nan_indices) > 0:
        events = _group_consecutive(nan_indices)
        for start, end in events:
            pitfalls.append(DetectedPitfall(
                pitfall_type="missing",
                start_idx=start,
                end_idx=end + 1,
                severity=(end - start + 1) / max(1, cfg["stuck_at_min_count"]),
                detail=f"NaN values from index {start} to {end}"
            ))

    return pitfalls


def _detect_stuck_at(values, cfg):
    """
    Detect STUCK-AT pitfall.
    Consecutive identical readings exceeding minimum count.
    """
    pitfalls = []
    min_count = cfg["stuck_at_min_count"]

    if len(values) < min_count:
        return pitfalls

    run_start = 0
    for i in range(1, len(values)):
        if np.isnan(values[i]) or np.isnan(values[run_start]):
            run_start = i
            continue
        if values[i] != values[run_start]:
            run_length = i - run_start
            if run_length >= min_count:
                pitfalls.append(DetectedPitfall(
                    pitfall_type="stuck-at",
                    start_idx=run_start,
                    end_idx=i,
                    severity=run_length / min_count,
                    detail=f"Value {values[run_start]:.4f} "
                           f"repeated {run_length} times"
                ))
            run_start = i

    # Check final run
    run_length = len(values) - run_start
    if run_length >= min_count and not np.isnan(values[run_start]):
        pitfalls.append(DetectedPitfall(
            pitfall_type="stuck-at",
            start_idx=run_start,
            end_idx=len(values),
            severity=run_length / min_count,
            detail=f"Value {values[run_start]:.4f} "
                   f"repeated {run_length} times (end of series)"
        ))

    return pitfalls


def _detect_spike(values, cfg):
    """
    Detect SPIKE pitfall.
    Values exceeding k standard deviations from rolling mean.
    """
    pitfalls = []
    z_thresh = cfg["spike_z_threshold"]
    window = cfg["rolling_window_size"]

    if np.sum(~np.isnan(values)) < window:
        return pitfalls

    rolling_mean = np.full_like(values, np.nan)
    rolling_std = np.full_like(values, np.nan)

    for i in range(window, len(values)):
        w = values[i - window:i]
        valid = w[~np.isnan(w)]
        if len(valid) > window // 2:
            rolling_mean[i] = np.mean(valid)
            rolling_std[i] = np.std(valid)

    valid_mask = (~np.isnan(rolling_mean) &
                  ~np.isnan(values) &
                  (rolling_std > 0))
    z_scores = np.full_like(values, 0.0)
    z_scores[valid_mask] = np.abs(
        (values[valid_mask] - rolling_mean[valid_mask]) /
        rolling_std[valid_mask]
    )

    spike_indices = np.where(z_scores > z_thresh)[0]

    if len(spike_indices) > 0:
        events = _group_consecutive(spike_indices, max_gap=3)
        for start, end in events:
            max_z = np.max(z_scores[start:end + 1])
            pitfalls.append(DetectedPitfall(
                pitfall_type="spike",
                start_idx=start,
                end_idx=end + 1,
                severity=max_z / z_thresh,
                detail=f"Max z-score: {max_z:.2f} "
                       f"(threshold: {z_thresh})"
            ))

    return pitfalls


def _detect_out_of_bound(values, bounds, cfg):
    """
    Detect OUT-OF-BOUND pitfall.
    Values outside automatically inferred bounds.
    """
    pitfalls = []
    lower, upper = bounds
    clean = values[~np.isnan(values)]

    if len(clean) < 10:
        return pitfalls

    oob_mask = ~np.isnan(values) & ((values < lower) | (values > upper))
    oob_indices = np.where(oob_mask)[0]

    if len(oob_indices) > 0:
        events = _group_consecutive(oob_indices, max_gap=3)
        for start, end in events:
            max_dev = np.max(np.abs(
                values[start:end + 1] -
                np.clip(values[start:end + 1], lower, upper)
            ))
            span = upper - lower if upper > lower else 1.0
            pitfalls.append(DetectedPitfall(
                pitfall_type="out-of-bound",
                start_idx=start,
                end_idx=end + 1,
                severity=max_dev / span,
                detail=f"Value outside auto-inferred bounds "
                       f"[{lower:.2f}, {upper:.2f}]"
            ))

    return pitfalls


def _detect_nonfunctional(values, intervals, expected_interval, cfg):
    """
    Detect NONFUNCTIONAL pitfall.
    Complete absence of readings exceeding permanent threshold.
    """
    pitfalls = []
    permanent_s = cfg["permanent_duration_minutes"] * 60

    if len(intervals) == 0:
        return pitfalls

    for i, gap in enumerate(intervals):
        if gap >= permanent_s:
            pitfalls.append(DetectedPitfall(
                pitfall_type="nonfunctional",
                start_idx=i,
                end_idx=i + 1,
                severity=gap / permanent_s,
                detail=f"No readings for {gap:.0f}s "
                       f"({gap / 60:.1f} min)"
            ))

    return pitfalls


def _detect_slow_response(intervals, expected_interval, cfg):
    """
    Detect SLOW-RESPONSE-TIME pitfall.
    Late arrivals: interval exceeds threshold but below nonfunctional.
    """
    pitfalls = []
    gap_threshold = expected_interval * cfg["gap_multiplier"]
    permanent_s = cfg["permanent_duration_minutes"] * 60

    if len(intervals) == 0:
        return pitfalls

    slow_mask = (intervals > gap_threshold) & (intervals < permanent_s)
    slow_indices = np.where(slow_mask)[0]

    if len(slow_indices) > 0:
        events = _group_consecutive(slow_indices)
        for start, end in events:
            max_delay = np.max(intervals[start:end + 1])
            pitfalls.append(DetectedPitfall(
                pitfall_type="slow-response-time",
                start_idx=start,
                end_idx=end + 1,
                severity=max_delay / gap_threshold,
                detail=f"Max delay: {max_delay:.1f}s "
                       f"(expected: {expected_interval:.1f}s)"
            ))

    return pitfalls


def _detect_degradation(values, cfg):
    """
    Detect DEGRADATION pitfall.
    Increasing noise over successive windows.
    """
    pitfalls = []
    window = cfg["rolling_window_size"]
    min_windows = cfg["progressive_min_windows"]

    clean = values[~np.isnan(values)]
    if len(clean) < window * min_windows:
        return pitfalls

    n_windows = len(clean) // window
    window_stds = []
    for i in range(n_windows):
        segment = clean[i * window:(i + 1) * window]
        window_stds.append(np.std(segment))

    window_stds = np.array(window_stds)

    if len(window_stds) < min_windows:
        return pitfalls

    diffs = np.diff(window_stds)
    increasing = diffs > 0

    run_start = None
    for i in range(len(increasing)):
        if increasing[i]:
            if run_start is None:
                run_start = i
        else:
            if run_start is not None and (i - run_start) >= min_windows:
                pitfalls.append(DetectedPitfall(
                    pitfall_type="degradation",
                    start_idx=run_start * window,
                    end_idx=(i + 1) * window,
                    severity=window_stds[i] / max(0.001, window_stds[run_start]),
                    detail=f"Noise increased from "
                           f"{window_stds[run_start]:.4f} to "
                           f"{window_stds[i]:.4f} over "
                           f"{i - run_start} windows"
                ))
            run_start = None

    if run_start is not None and (len(increasing) - run_start) >= min_windows:
        end_i = len(increasing)
        pitfalls.append(DetectedPitfall(
            pitfall_type="degradation",
            start_idx=run_start * window,
            end_idx=min(end_i * window, len(values)),
            severity=window_stds[end_i] / max(0.001, window_stds[run_start]),
            detail=f"Noise increased from "
                   f"{window_stds[run_start]:.4f} to "
                   f"{window_stds[end_i]:.4f} over "
                   f"{end_i - run_start} windows"
        ))

    return pitfalls


# ============================================================
# UTILITY
# ============================================================

def _group_consecutive(indices, max_gap=1):
    """Group consecutive or near-consecutive indices into (start, end) events."""
    if len(indices) == 0:
        return []

    events = []
    start = indices[0]
    prev = indices[0]

    for idx in indices[1:]:
        if idx - prev <= max_gap:
            prev = idx
        else:
            events.append((start, prev))
            start = idx
            prev = idx

    events.append((start, prev))
    return events
