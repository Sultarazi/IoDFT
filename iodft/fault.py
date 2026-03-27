"""
IoDFT Fault Class
Core object model implementing the six-facet IoDFT representation.
Each Fault instance encodes a single labelled fault event using the three-stage procedure.
"""

import json
from datetime import datetime
from iodft.taxonomy import (
    COMPONENTS, SOURCE_SCOPES, SOURCE_LOCATIONS,
    DURATIONS, TYPES, PITFALLS, CAUSES
)


class Fault:
    """
    Represents a single IoDFT-labelled fault event.

    Usage:
        fault = Fault()
        fault.set_component("hardware", detail="battery/solar subsystem")
        fault.set_source(scope="single", location="external")
        fault.set_duration("progressive", detail="10 weeks")
        fault.set_type("collective")
        fault.add_pitfall("missing")
        fault.add_pitfall("erroneous")
        fault.add_cause("battery_depletion")
        print(fault.label())
    """

    def __init__(self):
        # Stage 1: Base Facets
        self._component = None
        self._component_detail = None
        self._source_scope = None
        self._source_location = None
        self._source_detail = None
        self._duration = None
        self._duration_detail = None
        self._type = None
        self._type_detail = None

        # Stage 2: Pitfalls
        self._pitfalls = []
        self._pitfall_detail = None

        # Stage 3: Causes
        self._causes = []
        self._cause_detail = None

        # Metadata
        self._start_time = None
        self._end_time = None
        self._sensor_id = None
        self._created_at = datetime.utcnow().isoformat()

    # ========================================================
    # STAGE 1: Base Facets
    # ========================================================

    def set_component(self, component, detail=None):
        """Set the component facet (hardware, software, network)."""
        if component not in COMPONENTS:
            raise ValueError(
                f"Invalid component: '{component}'. "
                f"Valid: {list(COMPONENTS.keys())}"
            )
        self._component = component
        self._component_detail = detail
        return self

    def set_source(self, scope, location, detail=None):
        """Set the source facet (scope and location)."""
        if scope not in SOURCE_SCOPES:
            raise ValueError(
                f"Invalid scope: '{scope}'. "
                f"Valid: {list(SOURCE_SCOPES.keys())}"
            )
        if location not in SOURCE_LOCATIONS:
            raise ValueError(
                f"Invalid location: '{location}'. "
                f"Valid: {list(SOURCE_LOCATIONS.keys())}"
            )
        self._source_scope = scope
        self._source_location = location
        self._source_detail = detail
        return self

    def set_duration(self, duration, detail=None):
        """Set the duration facet (temporal, permanent, haphazard, progressive)."""
        if duration not in DURATIONS:
            raise ValueError(
                f"Invalid duration: '{duration}'. "
                f"Valid: {list(DURATIONS.keys())}"
            )
        self._duration = duration
        self._duration_detail = detail
        return self

    def set_type(self, fault_type, detail=None):
        """Set the type facet (point, contextual, collective)."""
        if fault_type not in TYPES:
            raise ValueError(
                f"Invalid type: '{fault_type}'. "
                f"Valid: {list(TYPES.keys())}"
            )
        self._type = fault_type
        self._type_detail = detail
        return self

    # ========================================================
    # STAGE 2: Data Pitfalls
    # ========================================================

    def add_pitfall(self, pitfall):
        """Add an observable data pitfall."""
        if pitfall not in PITFALLS:
            raise ValueError(
                f"Invalid pitfall: '{pitfall}'. "
                f"Valid: {list(PITFALLS.keys())}"
            )
        if pitfall not in self._pitfalls:
            self._pitfalls.append(pitfall)
        return self

    def set_pitfall_detail(self, detail):
        """Add descriptive detail for the pitfalls."""
        self._pitfall_detail = detail
        return self

    # ========================================================
    # STAGE 3: Possible Causes
    # ========================================================

    def add_cause(self, cause):
        """Add a hypothesised cause linked to the component."""
        valid_causes = CAUSES.get(self._component, [])
        all_causes = []
        for c_list in CAUSES.values():
            all_causes.extend(c_list)

        if cause not in all_causes:
            raise ValueError(
                f"Invalid cause: '{cause}'. "
                f"Valid for '{self._component}': {valid_causes}"
            )
        if cause not in self._causes:
            self._causes.append(cause)
        return self

    def set_cause_detail(self, detail):
        """Add descriptive detail for the causes."""
        self._cause_detail = detail
        return self

    # ========================================================
    # Metadata
    # ========================================================

    def set_time_window(self, start_time, end_time):
        """Set the temporal window of the fault event."""
        self._start_time = str(start_time)
        self._end_time = str(end_time)
        return self

    def set_sensor(self, sensor_id):
        """Set the sensor or device identifier."""
        self._sensor_id = sensor_id
        return self

    # ========================================================
    # Validation
    # ========================================================

    def is_valid(self):
        """Check whether all three stages are complete."""
        errors = []

        # Stage 1
        if self._component is None:
            errors.append("Stage 1: component not set")
        if self._source_scope is None or self._source_location is None:
            errors.append("Stage 1: source not set")
        if self._duration is None:
            errors.append("Stage 1: duration not set")
        if self._type is None:
            errors.append("Stage 1: type not set")

        # Stage 2
        if not self._pitfalls:
            errors.append("Stage 2: no pitfalls added")

        # Stage 3
        if not self._causes:
            errors.append("Stage 3: no causes added")

        return len(errors) == 0, errors

    # ========================================================
    # Output
    # ========================================================

    def label(self):
        """Generate a compact IoDFT label string."""
        comp = (self._component or "?").upper()
        loc = self._source_location or "?"
        scope = self._source_scope or "?"
        dur = self._duration or "?"
        ftype = self._type or "?"
        pitfalls = ", ".join(self._pitfalls) if self._pitfalls else "?"
        causes = ", ".join(self._causes) if self._causes else "?"

        return (
            f"[{comp} | {loc}-{scope} | {dur} | {ftype}] "
            f"Pitfall: {pitfalls} -> Cause: {causes}"
        )

    def to_dict(self):
        """Export the full annotation as a dictionary."""
        return {
            "stage_1": {
                "component": self._component,
                "component_detail": self._component_detail,
                "source": {
                    "scope": self._source_scope,
                    "location": self._source_location
                },
                "source_detail": self._source_detail,
                "duration": self._duration,
                "duration_detail": self._duration_detail,
                "type": self._type,
                "type_detail": self._type_detail
            },
            "stage_2": {
                "pitfall": self._pitfalls,
                "pitfall_detail": self._pitfall_detail
            },
            "stage_3": {
                "cause": self._causes,
                "cause_detail": self._cause_detail
            },
            "metadata": {
                "sensor_id": self._sensor_id,
                "start_time": self._start_time,
                "end_time": self._end_time,
                "created_at": self._created_at
            }
        }

    def to_json(self, filepath=None, indent=2):
        """Export annotation as JSON string or save to file."""
        data = self.to_dict()
        if filepath:
            with open(filepath, "w") as f:
                json.dump(data, f, indent=indent)
            return filepath
        return json.dumps(data, indent=indent)

    def summary(self):
        """Print a formatted summary of the fault annotation."""
        valid, errors = self.is_valid()
        status = "VALID" if valid else f"INCOMPLETE ({len(errors)} errors)"

        lines = [
            "=" * 60,
            "IoDFT Fault Annotation",
            "=" * 60,
            f"Status: {status}",
            "",
            "STAGE 1 — Base Facets:",
            f"  Component:  {self._component or 'NOT SET'}",
            f"              {self._component_detail or ''}",
            f"  Source:     {self._source_location or '?'}, {self._source_scope or '?'}",
            f"              {self._source_detail or ''}",
            f"  Duration:   {self._duration or 'NOT SET'}",
            f"              {self._duration_detail or ''}",
            f"  Type:       {self._type or 'NOT SET'}",
            f"              {self._type_detail or ''}",
            "",
            "STAGE 2 — Data Pitfalls:",
            f"  Pitfalls:   {', '.join(self._pitfalls) if self._pitfalls else 'NONE'}",
            f"              {self._pitfall_detail or ''}",
            "",
            "STAGE 3 — Possible Causes:",
            f"  Causes:     {', '.join(self._causes) if self._causes else 'NONE'}",
            f"              {self._cause_detail or ''}",
            "",
            "LABEL:",
            f"  {self.label()}",
            "=" * 60,
        ]

        output = "\n".join(lines)
        print(output)
        return output

    def __repr__(self):
        return f"Fault({()})"

    def __str__(self):
        return self.label()

    @classmethod
    def from_detections(cls, detections, sensor_id=None):
        """
        Create IoDFT Fault objects from external detector output.
        
        Allows any detector (Isolation Forest, TALIA, DQD, LSTM,
        or custom) to produce IoDFT-labelled faults without using
        the built-in detector.

        Parameters
        ----------
        detections : list of dict
            Each dict represents one detected event with keys:
            - 'pitfall' (required): str or list of str from IoDFT pitfalls
            - 'start_time' (optional): start timestamp or index
            - 'end_time' (optional): end timestamp or index
            - 'component' (optional): str, auto-inferred if not provided
            - 'scope' (optional): str, default 'single'
            - 'location' (optional): str, auto-inferred if not provided
            - 'duration' (optional): str, auto-inferred if not provided
            - 'type' (optional): str, auto-inferred if not provided
            - 'cause' (optional): str or list of str
            - 'detail' (optional): str description
        sensor_id : str, optional
            Sensor or device identifier.

        Returns
        -------
        list of Fault
            Fully labelled IoDFT Fault objects.

        Example
        -------
        # From Isolation Forest output
        my_detections = [
            {"pitfall": "spike", "start_time": "2023-10-01 03:14",
             "end_time": "2023-10-01 03:16"},
            {"pitfall": ["missing", "nonfunctional"],
             "start_time": "2023-12-13 15:29",
             "component": "hardware", "cause": "battery_depletion"},
        ]
        faults = Fault.from_detections(my_detections, sensor_id="MESH1760")

        # From TALIA output
        talia_events = [
            {"pitfall": "stuck-at", "start_time": 9416, "end_time": 9520,
             "duration": "temporal", "type": "collective"},
        ]
        faults = Fault.from_detections(talia_events, sensor_id="FIT401")
        """
        from iodft.taxonomy import COMPONENTS, PITFALLS, CAUSES

        faults = []
        for det in detections:
            fault = cls()

            # Parse pitfalls
            pitfalls = det.get("pitfall", [])
            if isinstance(pitfalls, str):
                pitfalls = [pitfalls]

            for p in pitfalls:
                try:
                    fault.add_pitfall(p)
                except ValueError:
                    pass

            if det.get("detail"):
                fault.set_pitfall_detail(det["detail"])

            # Component: use provided or infer from pitfalls
            component = det.get("component")
            if component and component in COMPONENTS:
                fault.set_component(component)
            else:
                component = cls._infer_component_from_pitfalls(pitfalls)
                fault.set_component(component)

            # Source
            scope = det.get("scope", "single")
            location = det.get("location")
            if not location:
                location = cls._infer_location_from_pitfalls(pitfalls)
            fault.set_source(scope=scope, location=location)

            # Duration: use provided or infer
            duration = det.get("duration")
            if duration:
                fault.set_duration(duration)
            else:
                duration = cls._infer_duration_from_pitfalls(pitfalls)
                fault.set_duration(duration)

            # Type: use provided or infer
            fault_type = det.get("type")
            if fault_type:
                fault.set_type(fault_type)
            else:
                fault_type = cls._infer_type_from_pitfalls(pitfalls)
                fault.set_type(fault_type)

            # Causes: use provided or infer
            causes = det.get("cause", [])
            if isinstance(causes, str):
                causes = [causes]
            if not causes:
                causes = cls._infer_causes_from_pitfalls(
                    pitfalls, component
                )
            for c in causes:
                try:
                    fault.add_cause(c)
                except ValueError:
                    pass

            # Metadata
            if sensor_id:
                fault.set_sensor(sensor_id)
            if det.get("start_time") and det.get("end_time"):
                fault.set_time_window(det["start_time"], det["end_time"])

            faults.append(fault)

        return faults

    @staticmethod
    def _infer_component_from_pitfalls(pitfalls):
        """Infer component from pitfall types."""
        pts = set(pitfalls)
        if "overwhelmed-traffic" in pts or "slow-response-time" in pts:
            return "network"
        if "erroneous" in pts and len(pts) == 1:
            return "software"
        return "hardware"

    @staticmethod
    def _infer_location_from_pitfalls(pitfalls):
        """Infer source location from pitfall types."""
        pts = set(pitfalls)
        if any(p in pts for p in ["nonfunctional", "degradation", "missing"]):
            return "external"
        return "internal"

    @staticmethod
    def _infer_duration_from_pitfalls(pitfalls):
        """Infer duration from pitfall types."""
        pts = set(pitfalls)
        if "nonfunctional" in pts:
            return "permanent"
        if "degradation" in pts:
            return "progressive"
        return "temporal"

    @staticmethod
    def _infer_type_from_pitfalls(pitfalls):
        """Infer fault type from pitfall types."""
        pts = set(pitfalls)
        if len(pts) > 1 or "degradation" in pts or "nonfunctional" in pts:
            return "collective"
        if "spike" in pts or "out-of-bound" in pts:
            return "point"
        return "contextual"

    @staticmethod
    def _infer_causes_from_pitfalls(pitfalls, component):
        """Infer possible causes from pitfalls and component."""
        from iodft.taxonomy import CAUSES
        pts = set(pitfalls)
        causes = []

        if component == "hardware":
            if "nonfunctional" in pts:
                causes.extend(["battery_depletion", "physical_damage"])
            if "degradation" in pts:
                causes.extend(["sensor_ageing", "sensor_degradation"])
            if "stuck-at" in pts:
                causes.append("sensor_degradation")
            if "spike" in pts:
                causes.append("environmental_exposure")
            if "out-of-bound" in pts:
                causes.append("calibration_drift")
            if "missing" in pts:
                causes.append("power_cycling")
        elif component == "network":
            if "slow-response-time" in pts:
                causes.extend(["congestion", "latency"])
            if "missing" in pts:
                causes.append("connectivity_loss")
            if "overwhelmed-traffic" in pts:
                causes.append("bandwidth_saturation")
        elif component == "software":
            if "erroneous" in pts:
                causes.append("configuration_error")
            causes.append("firmware_bug")

        # Deduplicate
        seen = set()
        unique = []
        for c in causes:
            if c not in seen:
                seen.add(c)
                unique.append(c)
        return unique[:4]

