"""
IoDFT — Automatic Detection and Labelling

Two modes of operation:

1. AUTOMATIC: Give it raw sensor data (CSV), get IoDFT labels
   python examples/example_auto_detect.py data.csv Timestamp Value

2. INTEGRATION: Feed your own detector's output, get IoDFT labels
   See from_detections() example below

If no CSV provided, runs on demo data.
"""

import sys
import os
import csv
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from iodft import detect_and_label
from iodft.fault import Fault


def load_csv(filepath, ts_col, val_col):
    """Load timestamps and values from any CSV file."""
    timestamps = []
    values = []

    with open(filepath, "r") as f:
        reader = csv.DictReader(f)

        if ts_col not in reader.fieldnames:
            print(f"Error: column '{ts_col}' not found.")
            print(f"Available columns: {reader.fieldnames}")
            sys.exit(1)
        if val_col not in reader.fieldnames:
            print(f"Error: column '{val_col}' not found.")
            print(f"Available columns: {reader.fieldnames}")
            sys.exit(1)

        for row in reader:
            try:
                ts = datetime.fromisoformat(
                    row[ts_col].replace("Z", "+00:00")
                )
                val = float(row[val_col])
                timestamps.append(ts)
                values.append(val)
            except (ValueError, KeyError):
                continue

    return timestamps, values


def demo_auto_detect():
    """Demonstrate fully automatic detection on raw data."""
    import numpy as np
    np.random.seed(42)

    n = 1440
    ts_seconds = np.cumsum(np.full(n, 60.0))
    base = 20 + 5 * np.sin(2 * np.pi * (ts_seconds / 3600 - 6) / 24)
    vals = base + np.random.normal(0, 0.3, n)

    # Inject faults
    vals[500:530] = vals[500]
    vals[700] = vals[700] + 40
    vals[1000:1003] = -15.0
    ts_seconds[180:300] = ts_seconds[179] + np.linspace(1, 7200, 120)

    return ts_seconds, vals


def demo_from_detections():
    """Demonstrate integration with external detectors."""
    print("\n" + "=" * 60)
    print("MODE 2: Integration with external detector")
    print("=" * 60)
    print("Using Fault.from_detections() to label external output\n")

    # Example: output from Isolation Forest, TALIA, or any detector
    my_detections = [
        {
            "pitfall": "spike",
            "start_time": "2023-10-01 03:14",
            "end_time": "2023-10-01 03:16"
        },
        {
            "pitfall": ["missing", "nonfunctional"],
            "start_time": "2023-12-13 15:00",
            "end_time": "2023-12-14 00:00",
            "component": "hardware",
            "cause": "battery_depletion"
        },
        {
            "pitfall": ["stuck-at", "out-of-bound"],
            "start_time": 9416,
            "end_time": 9520,
            "duration": "temporal",
            "type": "collective",
            "detail": "FIT401 spoofed to 0.5 during attack"
        },
        {
            "pitfall": "slow-response-time",
            "start_time": "2023-11-15 08:00",
            "end_time": "2023-11-15 08:30"
        },
        {
            "pitfall": "degradation",
            "start_time": "2023-11-01",
            "end_time": "2023-12-13"
        }
    ]

    faults = Fault.from_detections(my_detections, sensor_id="EXAMPLE")

    for i, fault in enumerate(faults, 1):
        print(f"Detection {i}: {fault.label()}")

    return faults


def main():
    if len(sys.argv) >= 4:
        filepath = sys.argv[1]
        ts_col = sys.argv[2]
        val_col = sys.argv[3]

        if not os.path.exists(filepath):
            print(f"Error: file '{filepath}' not found.")
            sys.exit(1)

        print(f"Loading: {filepath}")
        timestamps, values = load_csv(filepath, ts_col, val_col)
        sensor_id = os.path.basename(filepath).replace(".csv", "")
    else:
        print("No CSV provided. Running on demo data.")
        print("Usage: python examples/example_auto_detect.py data.csv ts_col val_col\n")

        import numpy as np
        timestamps, values = demo_auto_detect()
        sensor_id = "DEMO_SENSOR"

    print(f"Loaded {len(values)} readings")

    # ---- MODE 1: Fully automatic ----
    print("\n" + "=" * 60)
    print("MODE 1: Fully automatic detection and labelling")
    print("=" * 60)

    faults = detect_and_label(
        timestamps=timestamps,
        values=values,
        sensor_id=sensor_id
    )

    print(f"Detected {len(faults)} fault events\n")
    for i, fault in enumerate(faults[:10], 1):
        print(f"  {i}. {fault.label()}")

    # Save results
    output_file = "iodft_results.json"
    results = [fault.to_dict() for fault in faults]
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to: {output_file}")

    # ---- MODE 2: Integration ----
    demo_from_detections()


if __name__ == "__main__":
    main()
