# IoDFT — Internet of Data Faults Taxonomy

A structured semantic taxonomy and Python framework for consistent labelling and annotation of data-centric faults across IoT-Edge-Cloud (IEC) deployments.

## What IoDFT Is

IoDFT is a **labelling framework**, not a detector. It provides:

- A **formal six-facet taxonomy** for describing data faults in IEC systems
- A **three-stage annotation procedure** that produces consistent, machine-readable fault labels
- A **Python library** that operationalises the taxonomy for integration into any monitoring or detection pipeline

IoDFT addresses a gap identified across existing fault taxonomies: no single framework covers component attribution, source characterisation, temporal behaviour, structural type, observable manifestation, and hypothesised cause in a unified, faceted representation. Existing taxonomies typically address one or two of these dimensions. IoDFT integrates all six and provides a structured procedure for applying them.

## Why IoDFT

Detection algorithms (Isolation Forest, Matrix Profile, LSTM, etc.) output scores or binary flags. These outputs answer *"is this anomalous?"* but not:

- **What kind of fault is it?** (spike, stuck-at, missing, degradation, ...)
- **Where does it originate?** (sensor, network, software)
- **How long has it persisted?** (temporal, permanent, progressive)
- **What might cause it?** (battery depletion, calibration drift, congestion)
- **Which data quality dimension is affected?** (completeness, timeliness, consistency)

IoDFT answers these questions by labelling each detected event with a structured, six-facet annotation that is consistent across deployments, domains, and detection methods.

## Installation
```bash
git clone https://github.com/Sultarazi/IoDFT.git
cd IoDFT
pip install -e .
```

Requires Python 3.7+ and NumPy.

## Usage

### Integration with Any Detector (Primary Use)

IoDFT is designed to sit **on top of** any detection pipeline. Feed it your detector's output and get structured IoDFT labels:
```python
from iodft.fault import Fault

# Output from your detector (Isolation Forest, TALIA, DQD, custom, ...)
my_detections = [
    {"pitfall": "spike",
     "start_time": "2023-10-01 03:14",
     "end_time": "2023-10-01 03:16"},

    {"pitfall": ["missing", "nonfunctional"],
     "start_time": "2023-12-13",
     "end_time": "2023-12-14",
     "cause": "battery_depletion"},

    {"pitfall": ["stuck-at", "out-of-bound"],
     "start_time": 9416, "end_time": 9520,
     "duration": "temporal", "type": "collective",
     "detail": "FIT401 spoofed to 0.5 during attack"},
]

faults = Fault.from_detections(my_detections, sensor_id="MY_SENSOR")

for fault in faults:
    print(fault.label())
```

Output:
```
[HARDWARE | internal-single | temporal | point] Pitfall: spike -> Cause: environmental_exposure
[HARDWARE | external-single | permanent | collective] Pitfall: missing, nonfunctional -> Cause: battery_depletion
[HARDWARE | internal-single | temporal | collective] Pitfall: stuck-at, out-of-bound -> Cause: sensor_degradation, calibration_drift
```

Provide as little or as much as you want — IoDFT infers the remaining facets:

| You provide | IoDFT infers |
|---|---|
| `pitfall` only | component, source, duration, type, cause |
| `pitfall` + `component` | source, duration, type, cause |
| `pitfall` + `duration` + `type` | component, source, cause |
| All fields | Validates and structures the annotation |

### Standalone Pitfall Scanning

For rapid exploration, IoDFT includes a built-in pitfall scanner that can process raw time series directly. This is a convenience layer using rule-based methods; for production detection, integration with a dedicated detector (Mode 1 above) is recommended.
```python
from iodft import detect_and_label

faults = detect_and_label(
    timestamps=df["Timestamp"],
    values=df["Value"]
)

for fault in faults:
    print(fault.label())
```

The scanner infers sampling rate, value bounds, and window size from the data automatically. No configuration is required. Parameters can be overridden when needed:
```python
faults = detect_and_label(
    timestamps, values,
    config={
        "spike_z_threshold": 5.0,
        "permanent_duration_minutes": 60,
        "iqr_multiplier": 3.0,
    }
)
```

From the command line:
```bash
python examples/example_auto_detect.py sensor_data.csv Timestamp NO2
```

## Taxonomy

IoDFT characterises each fault through **six independent facets** applied in a **three-stage procedure**:

### Stage 1 — Base Facets (Origin and Structure)

| Facet | Categories | Role |
|-------|-----------|------|
| **Component** | `hardware`, `software`, `network` | Where the fault originates in the IEC system |
| **Source** | scope: `single` / `multi`; location: `internal` / `external` | Whether the fault is localised or propagated, internal or environmental |
| **Duration** | `temporal`, `permanent`, `haphazard`, `progressive` | The temporal persistence pattern of the fault |
| **Type** | `point`, `contextual`, `collective` | The structural characteristic of the anomalous pattern |

### Stage 2 — Data Pitfall (Observable Manifestation)

Ten categories capturing how the fault manifests in the data:

`missing` · `offset` · `stuck-at` · `spike` · `nonfunctional` · `degradation` · `out-of-bound` · `slow-response-time` · `erroneous` · `overwhelmed-traffic`

### Stage 3 — Possible Cause (Diagnostic Hypothesis)

Hypothesised causes linked to the component facet, guiding operators toward root-cause investigation. These are structured hypotheses, not confirmed diagnoses.

### Mapping to Data Quality Dimensions

IoDFT pitfalls map systematically to operational data quality dimensions:

| DQ Dimension | Related Pitfalls |
|---|---|
| **Completeness** | `missing`, `nonfunctional` |
| **Timeliness** | `slow-response-time`, `missing` |
| **Consistency** | `stuck-at`, `spike`, `out-of-bound`, `offset`, `erroneous` |

This mapping enables detection outputs to be reported in terms of data quality impact, supporting operational monitoring dashboards and provenance records.

## Output Format

Each labelled fault is a `Fault` object providing multiple representations:
```python
fault.label()       # Compact one-line IoDFT label
fault.summary()     # Formatted multi-line annotation summary
fault.to_dict()     # Full annotation as Python dictionary
fault.to_json()     # JSON export (string or file)
fault.is_valid()    # Validate all six facets are assigned
```

Example structured output:
```json
{
  "stage_1": {
    "component": "hardware",
    "source": {"scope": "single", "location": "external"},
    "duration": "progressive",
    "type": "collective"
  },
  "stage_2": {
    "pitfall": ["missing", "degradation"],
    "pitfall_detail": "Progressive data loss over 10 weeks"
  },
  "stage_3": {
    "cause": ["battery_depletion", "sensor_ageing"],
    "cause_detail": "Reduced solar irradiance at constrained urban site"
  }
}
```

## Architecture
```
iodft/
├── __init__.py       # Public API
├── taxonomy.py       # Six facets, valid categories, configuration defaults
├── fault.py          # Fault class: labelling, from_detections(), export
├── detector.py       # Built-in pitfall scanner (convenience layer)
└── labeller.py       # Automatic facet inference from detected patterns

schema/
└── IoDFT_schema.json # Formal taxonomy definition (machine-readable)

examples/
└── example_auto_detect.py  # CLI and integration demonstration
```

**Integration data flow (primary):**
```
Any detector output ──► Fault.from_detections() ──► IoDFT Fault objects
(scores, flags, events)   (infer missing facets)     (structured labels)
```

**Standalone data flow (convenience):**
```
Raw time series ──► detector.py ──► labeller.py ──► IoDFT Fault objects
(timestamps + values)  (scan pitfalls)  (infer facets)   (structured labels)
```

## Validated Across Domains

IoDFT has been validated on data from multiple IEC domains:

| Domain | Dataset | Readings | Faults detected | Key pitfalls |
|--------|---------|----------|-----------------|--------------|
| Environmental monitoring | Newcastle UO AQMesh NO₂ | 80,772 | 266 | spike, out-of-bound, degradation, nonfunctional |
| Industrial control | SWaT water treatment FIT401 | 14,997 | 282 | stuck-at, out-of-bound, spike |
| Vehicular communication | V2AIX RSU CAM broadcasts | — | Annotated | missing, slow-response-time |

## Formal Schema

The formal taxonomy is defined in [`schema/IoDFT_schema.json`](schema/IoDFT_schema.json) as a machine-readable JSON document. It specifies all valid facet categories and the three-stage procedure, enabling programmatic validation of annotations and interoperability between tools and deployments.

## Scanner Configuration

The built-in pitfall scanner parameters auto-scale from the data. All are overridable:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spike_z_threshold` | 3.0 | Z-score threshold for spike detection |
| `stuck_at_min_count` | 5 | Minimum consecutive identical readings |
| `gap_multiplier` | 5 | Gap threshold = multiplier × inferred sampling rate |
| `permanent_duration_minutes` | 20 | Duration threshold for permanent classification |
| `rolling_window_size` | auto | Auto-scaled to ~1 hour from inferred rate |
| `iqr_multiplier` | 1.5 | IQR multiplier for out-of-bound bounds |
| `progressive_min_windows` | 3 | Minimum windows for degradation detection |

## Citation

If you use IoDFT in your research, please cite:
```bibtex
@article{altarrazi2023addressing,
  author    = {Altarrazi, Sultan and Szydlo, Tomasz and Dustdar, Schahram
               and Srirama, Satish Narayana and Ranjan, Rajiv},
  title     = {Addressing the Faults Landscape in the Internet of Things:
               Toward Datacentric and System Resilience},
  journal   = {IEEE Internet Computing},
  volume    = {27},
  number    = {6},
  pages     = {43--51},
  year      = {2023}
}
```

## Licence

MIT License. See [LICENSE](LICENSE) for details.
