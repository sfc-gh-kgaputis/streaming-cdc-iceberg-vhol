import os
import sys

# Make the producer modules (common, cdc_simulator, telemetry, control, main)
# importable from one level up.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
