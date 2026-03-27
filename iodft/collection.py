"""
IoDFT Fault Collection
Manages a collection of IoDFT-labelled faults from one or more
sensors. Provides filtering, aggregation, statistical profiling,
and data quality dimension mapping.

Usage:
    from iodft import detect_and_label
    from iodft.collection import FaultCollection

    faults = detect_and_label(timestamps, values)
    collection = FaultCollection(faults)

    collection.profile()
    report = collection.dq_report(total_readings=80772)
"""

from iodft.fault import Fault


class FaultCollection:
    """
    A structured collection of IoDFT Fault objects.
    Supports querying, filtering, profiling, and DQ reporting.
    """

    def __init__(self, faults=None):
        """
        Parameters
        ----------
        faults : list of Fault, optional
            Initial list of Fault objects.
        """
        self._faults = list(faults) if faults else []

    def add(self, fault):
        """Add a single Fault to the collection."""
        if not isinstance(fault, Fault):
            raise TypeError("Expected a Fault object")
        self._faults.append(fault)
        return self

    def add_many(self, faults):
        """Add multiple Faults to the collection."""
        for f in faults:
            self.add(f)
        return self

    @property
    def size(self):
        """Number of faults in the collection."""
        return len(self._faults)

    @property
    def all(self):
        """Return all faults as a list."""
        return list(self._faults)

    # ========================================================
    # FILTERING
    # ========================================================

    def filter_by_pitfall(self, pitfall):
        """Return faults containing a specific pitfall."""
        results = []
        for f in self._faults:
            d = f.to_dict()
            if pitfall in d["stage_2"]["pitfall"]:
                results.append(f)
        return FaultCollection(results)

    def filter_by_component(self, component):
        """Return faults with a specific component."""
        results = []
        for f in self._faults:
            d = f.to_dict()
            if d["stage_1"]["component"] == component:
                results.append(f)
        return FaultCollection(results)

    def filter_by_duration(self, duration):
        """Return faults with a specific duration."""
        results = []
        for f in self._faults:
            d = f.to_dict()
            if d["stage_1"]["duration"] == duration:
                results.append(f)
        return FaultCollection(results)

    def filter_by_type(self, fault_type):
        """Return faults with a specific type."""
        results = []
        for f in self._faults:
            d = f.to_dict()
            if d["stage_1"]["type"] == fault_type:
                results.append(f)
        return FaultCollection(results)

    def filter_by_sensor(self, sensor_id):
        """Return faults for a specific sensor."""
        results = []
        for f in self._faults:
            d = f.to_dict()
            if d["metadata"]["sensor_id"] == sensor_id:
                results.append(f)
        return FaultCollection(results)

    # ========================================================
    # AGGREGATION
    # ========================================================

    def count_by_facet(self, facet):
        """
        Count faults grouped by a facet value.

        Parameters
        ----------
        facet : str
            One of: 'component', 'duration', 'type', 'pitfall',
            'scope', 'location', 'cause'

        Returns
        -------
        dict
            Mapping of facet value to count.
        """
        counts = {}

        for f in self._faults:
            d = f.to_dict()

            if facet == "pitfall":
                for p in d["stage_2"]["pitfall"]:
                    counts[p] = counts.get(p, 0) + 1
            elif facet == "cause":
                for c in d["stage_3"]["cause"]:
                    counts[c] = counts.get(c, 0) + 1
            elif facet == "component":
                v = d["stage_1"]["component"]
                counts[v] = counts.get(v, 0) + 1
            elif facet == "duration":
                v = d["stage_1"]["duration"]
                counts[v] = counts.get(v, 0) + 1
            elif facet == "type":
                v = d["stage_1"]["type"]
                counts[v] = counts.get(v, 0) + 1
            elif facet == "scope":
                v = d["stage_1"]["source"]["scope"]
                counts[v] = counts.get(v, 0) + 1
            elif facet == "location":
                v = d["stage_1"]["source"]["location"]
                counts[v] = counts.get(v, 0) + 1
            else:
                raise ValueError(
                    f"Unknown facet: '{facet}'. Valid: component, "
                    f"duration, type, pitfall, cause, scope, location"
                )

        return dict(sorted(counts.items(), key=lambda x: -x[1]))

    def unique_pitfalls(self):
        """Return set of all pitfall types in the collection."""
        pitfalls = set()
        for f in self._faults:
            d = f.to_dict()
            pitfalls.update(d["stage_2"]["pitfall"])
        return pitfalls

    def unique_components(self):
        """Return set of all component types in the collection."""
        return set(
            f.to_dict()["stage_1"]["component"] for f in self._faults
        )

    # ========================================================
    # DATA QUALITY REPORT
    # ========================================================

    def dq_report(self, total_readings=None):
        """
        Map the fault collection to data quality dimensions.

        Links IoDFT pitfalls to DQ dimensions:
          - Completeness: missing, nonfunctional
          - Timeliness: slow-response-time, missing
          - Consistency: stuck-at, spike, out-of-bound, offset, erroneous

        Parameters
        ----------
        total_readings : int, optional
            Total number of readings in the dataset. Used to
            compute fault rates. If None, only counts are reported.

        Returns
        -------
        DQReport
            Structured data quality report.
        """
        # DQ dimension mapping
        completeness_pitfalls = {"missing", "nonfunctional"}
        timeliness_pitfalls = {"slow-response-time", "missing"}
        consistency_pitfalls = {
            "stuck-at", "spike", "out-of-bound",
            "offset", "erroneous", "overwhelmed-traffic"
        }

        completeness_faults = []
        timeliness_faults = []
        consistency_faults = []

        for f in self._faults:
            d = f.to_dict()
            pitfalls = set(d["stage_2"]["pitfall"])

            if pitfalls & completeness_pitfalls:
                completeness_faults.append(f)
            if pitfalls & timeliness_pitfalls:
                timeliness_faults.append(f)
            if pitfalls & consistency_pitfalls:
                consistency_faults.append(f)

        return DQReport(
            total_faults=self.size,
            total_readings=total_readings,
            completeness_faults=FaultCollection(completeness_faults),
            timeliness_faults=FaultCollection(timeliness_faults),
            consistency_faults=FaultCollection(consistency_faults),
            pitfall_counts=self.count_by_facet("pitfall"),
            duration_counts=self.count_by_facet("duration"),
            component_counts=self.count_by_facet("component"),
        )

    # ========================================================
    # PROFILE AND DISPLAY
    # ========================================================

    def profile(self):
        """Print a structured profile of the fault collection."""
        lines = [
            "=" * 60,
            "IoDFT Fault Collection Profile",
            "=" * 60,
            f"Total fault events: {self.size}",
            "",
            "Pitfall distribution:",
        ]
        for p, c in self.count_by_facet("pitfall").items():
            bar = "█" * min(50, int(50 * c / max(1, self.size)))
            lines.append(f"  {p:<25} {c:>5}  {bar}")

        lines.append("")
        lines.append("Duration distribution:")
        for d, c in self.count_by_facet("duration").items():
            bar = "█" * min(50, int(50 * c / max(1, self.size)))
            lines.append(f"  {d:<25} {c:>5}  {bar}")

        lines.append("")
        lines.append("Component distribution:")
        for comp, c in self.count_by_facet("component").items():
            bar = "█" * min(50, int(50 * c / max(1, self.size)))
            lines.append(f"  {comp:<25} {c:>5}  {bar}")

        lines.append("")
        lines.append("Type distribution:")
        for t, c in self.count_by_facet("type").items():
            bar = "█" * min(50, int(50 * c / max(1, self.size)))
            lines.append(f"  {t:<25} {c:>5}  {bar}")

        lines.append("=" * 60)

        output = "\n".join(lines)
        print(output)
        return output

    def to_list(self):
        """Export all faults as a list of dictionaries."""
        return [f.to_dict() for f in self._faults]

    def to_json(self, filepath=None, indent=2):
        """Export all faults as JSON."""
        import json
        data = self.to_list()
        if filepath:
            with open(filepath, "w") as f:
                json.dump(data, f, indent=indent, default=str)
            return filepath
        return json.dumps(data, indent=indent, default=str)

    def __len__(self):
        return self.size

    def __iter__(self):
        return iter(self._faults)

    def __repr__(self):
        return f"FaultCollection(size={self.size})"


class DQReport:
    """
    Data Quality Report generated from an IoDFT fault collection.
    Maps faults to completeness, timeliness, and consistency dimensions.
    """

    def __init__(self, total_faults, total_readings,
                 completeness_faults, timeliness_faults,
                 consistency_faults, pitfall_counts,
                 duration_counts, component_counts):
        self.total_faults = total_faults
        self.total_readings = total_readings
        self.completeness = completeness_faults
        self.timeliness = timeliness_faults
        self.consistency = consistency_faults
        self._pitfall_counts = pitfall_counts
        self._duration_counts = duration_counts
        self._component_counts = component_counts

    @property
    def completeness_count(self):
        """Number of faults affecting completeness."""
        return self.completeness.size

    @property
    def timeliness_count(self):
        """Number of faults affecting timeliness."""
        return self.timeliness.size

    @property
    def consistency_count(self):
        """Number of faults affecting consistency."""
        return self.consistency.size

    @property
    def completeness_rate(self):
        """Completeness fault rate (faults / total readings)."""
        if self.total_readings:
            return 1.0 - (self.completeness_count / self.total_readings)
        return None

    @property
    def timeliness_rate(self):
        """Timeliness fault rate."""
        if self.total_readings:
            return 1.0 - (self.timeliness_count / self.total_readings)
        return None

    @property
    def consistency_rate(self):
        """Consistency fault rate."""
        if self.total_readings:
            return 1.0 - (self.consistency_count / self.total_readings)
        return None

    def summary(self):
        """Print a formatted DQ report."""
        lines = [
            "=" * 60,
            "IoDFT Data Quality Report",
            "=" * 60,
            f"Total readings:     {self.total_readings or 'N/A'}",
            f"Total fault events: {self.total_faults}",
            "",
            "Data Quality Dimensions:",
            f"  Completeness:  {self.completeness_count} fault events "
            f"affecting completeness",
        ]
        if self.completeness_rate is not None:
            lines.append(
                f"                 Rate: {self.completeness_rate:.4f} "
                f"({self.completeness_rate * 100:.2f}%)"
            )
        lines.append(
            f"                 Pitfalls: missing, nonfunctional"
        )

        lines.append(
            f"  Timeliness:    {self.timeliness_count} fault events "
            f"affecting timeliness"
        )
        if self.timeliness_rate is not None:
            lines.append(
                f"                 Rate: {self.timeliness_rate:.4f} "
                f"({self.timeliness_rate * 100:.2f}%)"
            )
        lines.append(
            f"                 Pitfalls: slow-response-time, missing"
        )

        lines.append(
            f"  Consistency:   {self.consistency_count} fault events "
            f"affecting consistency"
        )
        if self.consistency_rate is not None:
            lines.append(
                f"                 Rate: {self.consistency_rate:.4f} "
                f"({self.consistency_rate * 100:.2f}%)"
            )
        lines.append(
            f"                 Pitfalls: stuck-at, spike, out-of-bound, "
            f"offset, erroneous"
        )

        lines.extend([
            "",
            "Dominant patterns:",
            f"  Most common pitfall:   "
            f"{list(self._pitfall_counts.keys())[0] if self._pitfall_counts else 'N/A'}",
            f"  Most common duration:  "
            f"{list(self._duration_counts.keys())[0] if self._duration_counts else 'N/A'}",
            f"  Most common component: "
            f"{list(self._component_counts.keys())[0] if self._component_counts else 'N/A'}",
            "=" * 60,
        ])

        output = "\n".join(lines)
        print(output)
        return output

    def to_dict(self):
        """Export the DQ report as a dictionary."""
        return {
            "total_readings": self.total_readings,
            "total_faults": self.total_faults,
            "dimensions": {
                "completeness": {
                    "fault_count": self.completeness_count,
                    "rate": self.completeness_rate,
                    "related_pitfalls": ["missing", "nonfunctional"]
                },
                "timeliness": {
                    "fault_count": self.timeliness_count,
                    "rate": self.timeliness_rate,
                    "related_pitfalls": [
                        "slow-response-time", "missing"
                    ]
                },
                "consistency": {
                    "fault_count": self.consistency_count,
                    "rate": self.consistency_rate,
                    "related_pitfalls": [
                        "stuck-at", "spike", "out-of-bound",
                        "offset", "erroneous", "overwhelmed-traffic"
                    ]
                }
            },
            "pitfall_counts": self._pitfall_counts,
            "duration_counts": self._duration_counts,
            "component_counts": self._component_counts
        }

    def __repr__(self):
        return (
            f"DQReport(faults={self.total_faults}, "
            f"completeness={self.completeness_count}, "
            f"timeliness={self.timeliness_count}, "
            f"consistency={self.consistency_count})"
        )
