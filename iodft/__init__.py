"""
IoDFT — Internet of Data Faults Taxonomy
A structured semantic taxonomy for detecting and labelling
data-centric faults in IoT-Edge-Cloud (IEC) deployments.

Usage:
    from iodft import detect_and_label

    faults = detect_and_label(
        timestamps=df["timestamp"],
        values=df["value"],
        expected_interval=60
    )

    for f in faults:
        print(f.label())
"""

from iodft.fault import Fault
from iodft.detector import detect_pitfalls
from iodft.labeller import detect_and_label

__version__ = "1.0.1"
__all__ = ["Fault", "detect_pitfalls", "detect_and_label"]
