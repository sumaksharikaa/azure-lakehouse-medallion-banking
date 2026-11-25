"""Thin launcher used by the ADF DatabricksSparkPython activity (the wheel provides `lakehouse`)."""

from lakehouse.pipeline import main

if __name__ == "__main__":
    main()
