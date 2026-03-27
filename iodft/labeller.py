"""
IoDFT Automatic Labeller
Takes detected pitfalls from detector.py and automatically
assigns full IoDFT labels (all six facets) based on the
patterns found in the data.

This is the core automation: data in → IoDFT labels out.

Usage:
    from iodft import detect_and_label

    faults = detect_and_label(timestamps, values)
    for f in faults:
        print(f.label())
"""

import numpy as np
from iodft.fault import Fault
from iodft.detector import detect_pitfalls, DetectedPitfall, _compute_intervals


def detect_and_label(timestamps, values, expected_interval=None,
                     config=None, sensor_id=None):
    """
    Fully automatic: detect pitfalls and assign IoDFT labels.

    Parameters
    ----------
    timestamps : array-like
        Timestamps as datetime objects, numpy datetime64, or numeric.
    values : array-like
        Sensor readings as numeric values.
    expected_interval : float, optional
        Expected sampling interval in seconds. Auto-inferred if None.
    config : dict, optional
        Override detection parameters.
    sensor_id : str, optional
        Identifier for the sensor or device.

    Returns
    -------
    list of Fault
        Fully labelled IoDFT Fault objects.
    """
    # Step 1: Detect pitfalls
    pitfalls = detect_pitfalls(timestamps, values, expected_interval, config)

    if not pitfalls:
        return []

    # Step 2: Group pitfalls into fault events
    events = _group_into_events(pitfalls, values, config)

    # Step 3: Label each event with full IoDFT facets
    faults = []
    for event in events:
        fault = _label_event(event, values, timestamps, sensor_id)
        faults.append(fault)

    return faults


def _group_into_events(pitfalls, values, config=None):
    """
    Group individual pitfalls into fault events.
    Pitfalls that overlap or are close in time belong to the same event.
    A single fault event may contain multiple pitfall types.
    """
    if not pitfalls:
        return []

    cfg = config or {}
    merge_gap = cfg.get("event_merge_gap", 10)

    # Sort by start index
    sorted_pitfalls = sorted(pitfalls, key=lambda p: p.start_idx)

    events = []
    current_event = {
        "pitfalls": [sorted_pitfalls[0]],
        "start_idx": sorted_pitfalls[0].start_idx,
        "end_idx": sorted_pitfalls[0].end_idx,
        "pitfall_types": {sorted_pitfalls[0].pitfall_type},
        "max_severity": sorted_pitfalls[0].severity
    }

    for p in sorted_pitfalls[1:]:
        # Merge if overlapping or within merge gap
        if p.start_idx <= current_event["end_idx"] + merge_gap:
            current_event["pitfalls"].append(p)
            current_event["end_idx"] = max(current_event["end_idx"], p.end_idx)
            current_event["pitfall_types"].add(p.pitfall_type)
            current_event["max_severity"] = max(
                current_event["max_severity"], p.severity
            )
        else:
            events.append(current_event)
            current_event = {
                "pitfalls": [p],
                "start_idx": p.start_idx,
                "end_idx": p.end_idx,
                "pitfall_types": {p.pitfall_type},
                "max_severity": p.severity
            }

    events.append(current_event)
    return events


def _label_event(event, values, timestamps, sensor_id=None):
    """
    Assign full IoDFT six-facet label to a fault event
    based on the detected pitfall patterns.
    """
    fault = Fault()

    pitfall_types = event["pitfall_types"]
    start_idx = event["start_idx"]
    end_idx = event["end_idx"]
    duration_points = end_idx - start_idx
    max_severity = event["max_severity"]

    # ====================================================
    # STAGE 1: Base Facets (inferred from patterns)
    # ====================================================

    # Component: inferred from pitfall types
    component, component_detail = _infer_component(pitfall_types)
    fault.set_component(component, detail=component_detail)

    # Source: inferred from scope of the event
    scope, location, source_detail = _infer_source(
        event, values, timestamps
    )
    fault.set_source(scope=scope, location=location, detail=source_detail)

    # Duration: inferred from event length and pattern
    duration, duration_detail = _infer_duration(
        event, values, timestamps
    )
    fault.set_duration(duration, detail=duration_detail)

    # Type: inferred from event structure
    fault_type, type_detail = _infer_type(event, values)
    fault.set_type(fault_type, detail=type_detail)

    # ====================================================
    # STAGE 2: Pitfalls (from detector)
    # ====================================================

    for p_type in pitfall_types:
        fault.add_pitfall(p_type)

    details = [p.detail for p in event["pitfalls"] if p.detail]
    if details:
        fault.set_pitfall_detail("; ".join(details[:3]))

    # ====================================================
    # STAGE 3: Causes (hypothesised from patterns)
    # ====================================================

    causes = _infer_causes(pitfall_types, component, duration)
    for cause in causes:
        try:
            fault.add_cause(cause)
        except ValueError:
            pass

    # ====================================================
    # Metadata
    # ====================================================

    if sensor_id:
        fault.set_sensor(sensor_id)

    try:
        ts_array = np.array(timestamps)
        if start_idx < len(ts_array) and end_idx <= len(ts_array):
            fault.set_time_window(
                ts_array[start_idx],
                ts_array[min(end_idx, len(ts_array) - 1)]
            )
    except (IndexError, TypeError):
        pass

    return fault


# ============================================================
# FACET INFERENCE RULES
# ============================================================

def _infer_component(pitfall_types):
    """
    Infer the component facet from observed pitfall types.

    Rules:
    - missing + slow-response-time → network
    - nonfunctional → hardware
    - stuck-at → hardware (sensor)
    - spike + out-of-bound → hardware (sensor)
    - degradation → hardware (sensor ageing)
    - overwhelmed-traffic → network
    - erroneous alone → software
    - default → hardware
    """
    pts = pitfall_types

    if "overwhelmed-traffic" in pts:
        return "network", "Communication layer congestion or buffer overflow"

    if "nonfunctional" in pts:
        return "hardware", "Device ceased operation entirely"

    if "slow-response-time" in pts and "missing" in pts:
        return "network", "Transmission delays causing data gaps"

    if "slow-response-time" in pts:
        return "network", "Delayed data transmission"

    if "degradation" in pts:
        return "hardware", "Progressive sensor or power degradation"

    if "stuck-at" in pts:
        return "hardware", "Sensor producing constant readings"

    if "spike" in pts or "out-of-bound" in pts:
        return "hardware", "Sensor producing anomalous values"

    if "erroneous" in pts and len(pts) == 1:
        return "software", "Possible misconfiguration or processing error"

    if "offset" in pts:
        return "hardware", "Sensor calibration or measurement offset"

    if "missing" in pts:
        return "hardware", "Device failed to transmit readings"

    return "hardware", "Unspecified device-layer fault"


def _infer_source(event, values, timestamps):
    """
    Infer the source facet (scope and location).

    Rules:
    - Single event in data → single scope
    - Nonfunctional/degradation → likely external
    - Spike/erroneous → likely internal
    """
    pitfall_types = event["pitfall_types"]

    # Scope: always single for single-sensor analysis
    # Multi-sensor detection would require cross-sensor input
    scope = "single"

    # Location
    if any(p in pitfall_types for p in
           ["nonfunctional", "degradation", "missing"]):
        location = "external"
        detail = "Likely environmental or site-specific factors"
    elif any(p in pitfall_types for p in
             ["stuck-at", "spike", "erroneous", "offset"]):
        location = "internal"
        detail = "Likely internal device or software fault"
    elif "overwhelmed-traffic" in pitfall_types:
        location = "external"
        detail = "External network conditions"
    else:
        location = "internal"
        detail = "Default attribution"

    return scope, location, detail


def _infer_duration(event, values, timestamps):
    """
    Infer the duration facet from event characteristics.

    Rules:
    - duration_points <= short_threshold → temporal
    - nonfunctional pitfall → permanent
    - degradation pitfall → progressive
    - duration_points > long_threshold → permanent
    - recurring pattern → haphazard
    """
    pitfall_types = event["pitfall_types"]
    duration_points = event["end_idx"] - event["start_idx"]
    n_total = len(values)

    # Fraction of total data
    fraction = duration_points / max(1, n_total)

    if "nonfunctional" in pitfall_types:
        return "permanent", f"Device offline ({duration_points} points)"

    if "degradation" in pitfall_types:
        return "progressive", (
            f"Gradual degradation over {duration_points} points "
            f"({fraction * 100:.1f}% of series)"
        )

    # Check if multiple separate pitfalls of same type exist
    type_counts = {}
    for p in event["pitfalls"]:
        type_counts[p.pitfall_type] = type_counts.get(p.pitfall_type, 0) + 1

    has_recurring = any(c > 2 for c in type_counts.values())

    if has_recurring and fraction < 0.5:
        return "haphazard", (
            f"Recurring pattern: {type_counts}, "
            f"spanning {duration_points} points"
        )

    if fraction > 0.3:
        return "permanent", (
            f"Sustained fault covering {fraction * 100:.1f}% of series"
        )

    if duration_points <= 5:
        return "temporal", f"Brief event ({duration_points} points)"

    return "temporal", f"Recoverable event ({duration_points} points)"


def _infer_type(event, values):
    """
    Infer the fault type from event structure.

    Rules:
    - Single point → point
    - Multiple points but context-dependent → contextual
    - Sustained sequence → collective
    """
    duration_points = event["end_idx"] - event["start_idx"]
    n_pitfalls = len(event["pitfalls"])

    if duration_points <= 1:
        return "point", "Isolated single-point anomaly"

    if duration_points <= 5 and n_pitfalls <= 2:
        return "point", f"Brief anomaly ({duration_points} points)"

    if duration_points > 5 and len(event["pitfall_types"]) == 1:
        p = list(event["pitfall_types"])[0]
        if p in ["spike", "out-of-bound"]:
            return "contextual", (
                f"Anomaly in context ({duration_points} points, "
                f"pitfall: {p})"
            )

    return "collective", (
        f"Sustained anomaly sequence ({duration_points} points, "
        f"{len(event['pitfall_types'])} pitfall types)"
    )


def _infer_causes(pitfall_types, component, duration):
    """
    Hypothesise possible causes based on component, pitfalls, and duration.
    Returns a list of plausible cause strings.
    """
    causes = []

    if component == "hardware":
        if "nonfunctional" in pitfall_types:
            causes.append("battery_depletion")
            causes.append("physical_damage")
        if "degradation" in pitfall_types:
            causes.append("sensor_ageing")
            causes.append("sensor_degradation")
        if "stuck-at" in pitfall_types:
            causes.append("sensor_degradation")
        if "spike" in pitfall_types:
            causes.append("environmental_exposure")
        if "out-of-bound" in pitfall_types:
            causes.append("calibration_drift")
        if "missing" in pitfall_types:
            causes.append("power_cycling")
        if "offset" in pitfall_types:
            causes.append("calibration_drift")
        if duration == "progressive":
            if "battery_depletion" not in causes:
                causes.append("battery_depletion")
            if "solar_panel_obstruction" not in causes:
                causes.append("solar_panel_obstruction")

    elif component == "network":
        if "missing" in pitfall_types:
            causes.append("connectivity_loss")
        if "slow-response-time" in pitfall_types:
            causes.append("congestion")
            causes.append("latency")
        if "overwhelmed-traffic" in pitfall_types:
            causes.append("bandwidth_saturation")
            causes.append("broker_overload")

    elif component == "software":
        if "erroneous" in pitfall_types:
            causes.append("configuration_error")
        if "out-of-bound" in pitfall_types:
            causes.append("unit_conversion_error")
        causes.append("firmware_bug")

    # Deduplicate
    seen = set()
    unique = []
    for c in causes:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    return unique[:4]
