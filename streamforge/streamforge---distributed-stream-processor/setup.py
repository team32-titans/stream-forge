try:
    from setuptools import setup, find_packages
except ImportError:
    setup = None
    find_packages = None

if setup and find_packages:
    setup(
        name="streamforge",
        version="1.0.0",
        packages=find_packages(),
        install_requires=[
            "pydantic>=2.0.0",
            "pydantic-settings>=2.0.0",
            "prometheus-client>=0.19.0",
            "typing-extensions>=4.8.0",
        ],
        entry_points={
            "console_scripts": [
                "streamforge=main:main",
            ],
        },
        python_requires=">=3.9",
    )
