import os
import sys

# Make `import producer` find producer.py one level up from this directory.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
