from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def emit(value):
    print(json.dumps(value, ensure_ascii=False, default=str, sort_keys=True))
