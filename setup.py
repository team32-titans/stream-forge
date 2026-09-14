from setuptools import setup, find_packages

setup(
    name="streamforge",
    version="1.0.0",
    packages=find_packages(exclude=("s1*", "tests*", "streamforge---*")),
    install_requires=[
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
        "prometheus-client>=0.19.0",
        "confluent-kafka>=2.4.0",
        "typing-extensions>=4.8.0",
    ],

    entry_points={
        "console_scripts": [
            "streamforge=streamforge.cli:main",
        ],
    },
    python_requires=">=3.9",
)
