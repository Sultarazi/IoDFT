from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="iodft",
    version="1.0.1",
    author="Sultan Altarrazi",
    description="Internet of Data Faults Taxonomy — automatic detection and structured labelling of data-centric faults in IEC deployments",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Sultarazi/IoDFT",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "numpy>=1.20.0",
    ],
    keywords="iot, data quality, fault taxonomy, anomaly detection, iodft, iec",
)
