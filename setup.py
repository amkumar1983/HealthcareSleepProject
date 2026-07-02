from setuptools import find_packages, setup

setup(
    name="sleepapnea_dp",
    version="0.1.0",
    description="Reusable PySpark transformation library for the Sleep Apnea Device Data Platform",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=[
        "pyyaml>=6.0",
    ],
)
