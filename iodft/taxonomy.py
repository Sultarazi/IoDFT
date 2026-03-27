"""
IoDFT Taxonomy Definition
Defines the six facets, their valid categories, and the
three-stage annotation procedure.

Based on: Altarrazi et al., IEEE Internet Computing, 27(6), 2023.
"""


# ============================================================
# STAGE 1: Base Facets
# ============================================================

COMPONENTS = {
    "hardware": {
        "description": "Physical device faults",
        "sub_components": [
            "sensor", "actuator", "battery", "solar_panel",
            "memory", "processor", "antenna", "gateway"
        ]
    },
    "software": {
        "description": "Software or firmware faults",
        "sub_components": [
            "firmware", "configuration", "protocol", "parser",
            "driver", "middleware", "application", "scheduler"
        ]
    },
    "network": {
        "description": "Communication and transmission faults",
        "sub_components": [
            "wireless", "wired", "broker", "router",
            "gateway", "broadcast_stack", "cellular", "lorawan"
        ]
    }
}

SOURCE_SCOPES = {
    "single": "Fault affects one node or device",
    "multi": "Fault propagates across multiple nodes or devices"
}

SOURCE_LOCATIONS = {
    "internal": "Fault arises from within the device or system",
    "external": "Fault arises from environmental or external factors"
}

DURATIONS = {
    "temporal": {
        "description": "Short-lived and recoverable without intervention",
        "detection_rule": "Fault duration below the permanent threshold"
    },
    "permanent": {
        "description": "Sustained and systemic, requires maintenance",
        "detection_rule": "Fault duration exceeds the permanent threshold"
    },
    "haphazard": {
        "description": "Irregular with no clear pattern, intermittent",
        "detection_rule": "Fault recurs with non-periodic intervals"
    },
    "progressive": {
        "description": "Gradual degradation transitioning toward permanent",
        "detection_rule": "Fault severity increases over successive windows"
    }
}

TYPES = {
    "point": {
        "description": "Singular outlier at a single timestamp",
        "detection_rule": "Isolated deviation not preceded or followed by faults"
    },
    "contextual": {
        "description": "Value deviates only under specific conditions",
        "detection_rule": "Deviation relative to expected context or neighbours"
    },
    "collective": {
        "description": "Sequence of observations forming an anomaly over time",
        "detection_rule": "Consecutive or clustered fault occurrences"
    }
}

# ============================================================
# STAGE 2: Data Pitfalls
# ============================================================

PITFALLS = {
    "missing": {
        "description": "Expected data not transmitted or received",
        "detection": "Null values or gaps exceeding expected interval"
    },
    "offset": {
        "description": "Systematic shift from true value",
        "detection": "Sustained deviation from reference or calibration baseline"
    },
    "stuck-at": {
        "description": "Constant value maintained over a duration",
        "detection": "Zero variance over consecutive readings"
    },
    "spike": {
        "description": "Sudden rise or fall from the norm",
        "detection": "Value exceeds k standard deviations from rolling mean"
    },
    "nonfunctional": {
        "description": "Device produces no output at all",
        "detection": "Complete absence of readings over extended period"
    },
    "degradation": {
        "description": "Gradual decline in measurement quality",
        "detection": "Increasing noise, drift, or error rate over time"
    },
    "out-of-bound": {
        "description": "Value outside the physically plausible range",
        "detection": "Value exceeds domain-defined min/max bounds"
    },
    "slow-response-time": {
        "description": "Delayed data transmission or processing",
        "detection": "Inter-reading interval exceeds threshold but data arrives"
    },
    "erroneous": {
        "description": "Incorrect value that does not match expected behaviour",
        "detection": "Logical constraint violation or cross-sensor inconsistency"
    },
    "overwhelmed-traffic": {
        "description": "Data loss due to network congestion or buffer overflow",
        "detection": "Burst arrivals followed by gaps or duplicates"
    }
}

# ============================================================
# STAGE 3: Possible Causes (linked to Component)
# ============================================================

CAUSES = {
    "hardware": [
        "sensor_degradation", "sensor_ageing", "battery_depletion",
        "solar_panel_obstruction", "physical_damage", "calibration_drift",
        "memory_failure", "overheating", "environmental_exposure",
        "power_cycling", "cross_sensitivity"
    ],
    "software": [
        "firmware_bug", "configuration_error", "unit_conversion_error",
        "schema_change", "buffer_overflow", "parsing_error",
        "protocol_mismatch", "scheduling_error", "update_failure"
    ],
    "network": [
        "congestion", "signal_interference", "frequency_overlap",
        "gateway_buffering", "retransmission_failure", "coverage_gap",
        "bandwidth_saturation", "packet_loss", "latency",
        "broker_overload", "connectivity_loss"
    ]
}

# ============================================================
# Detection Defaults
# ============================================================

DEFAULT_CONFIG = {
    "spike_z_threshold": 3.0,
    "stuck_at_min_count": 5,
    "gap_multiplier": 5,
    "permanent_duration_minutes": 20,
    "rolling_window_size": 60,
    "out_of_bound_method": "iqr",
    "iqr_multiplier": 1.5,
    "progressive_min_windows": 3
}


def get_valid_values(facet):
    """Return valid values for a given facet name."""
    facet_map = {
        "component": list(COMPONENTS.keys()),
        "scope": list(SOURCE_SCOPES.keys()),
        "location": list(SOURCE_LOCATIONS.keys()),
        "duration": list(DURATIONS.keys()),
        "type": list(TYPES.keys()),
        "pitfall": list(PITFALLS.keys()),
    }
    if facet in facet_map:
        return facet_map[facet]
    raise ValueError(f"Unknown facet: '{facet}'. Valid facets: {list(facet_map.keys())}")


def get_causes_for_component(component):
    """Return valid causes for a given component."""
    if component in CAUSES:
        return CAUSES[component]
    raise ValueError(f"Unknown component: '{component}'. Valid: {list(CAUSES.keys())}")
