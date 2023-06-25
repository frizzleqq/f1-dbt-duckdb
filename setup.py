from pathlib import Path

from setuptools import setup

HERE = Path(__file__).parent.resolve()
LONG_DESCRIPTION = (HERE / "README.md").read_text(encoding="utf-8")
INSTALL_REQUIRES = (HERE / "requirements.txt").read_text("utf-8").splitlines()

DEV_REQUIREMENTS = [
    # development & testing tools
    "black>=23.0.0, <24.0.0",
    "isort>=5.0.0, <6.0.0",
    "mypy>=1.0.0, <2.0.0",
    "ruff",
    "sqlfluff-templater-dbt>=2.0.0, <3.0.0",
]

setup(
    name="f1dbtduckdb",
    version="0.1.0",
    packages=["foneload"],
    entry_points={"console_scripts": ["foneload = foneload.__main__:main"]},
    python_requires=">=3.10",
    setup_requires=["setuptools", "wheel"],
    install_requires=INSTALL_REQUIRES,
    extras_require={"dev": DEV_REQUIREMENTS},
    description="Experimenting with dbt/DuckDB using Ergast API as main source",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/frizzleqq/f1-dbt-duckdb",
    classifiers=[
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
