#!/usr/bin/env python
"""Root execution script for Groww Daily Candle Collection System."""

import sys
import os

# Ensure src is in Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

from groww_collector.cli import main

if __name__ == "__main__":
    main()
