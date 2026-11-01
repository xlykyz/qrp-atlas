"""One real Raw write for a transient service identity/namespace probe."""
import errno
import json
from pathlib import Path
import sys

import pandas as pd
from qrp_atlas.pipeline.lake.raw import RawStore

try:
    batch = RawStore(Path(sys.argv[1])).save_frame("fixture", "daily",
        pd.DataFrame({"original": [1]}), parameters={}, complete=True)
    print(json.dumps({"batch_id": batch.batch_id}))
except OSError as exc:
    print(json.dumps({"errno": exc.errno}), file=sys.stderr)
    raise SystemExit(exc.errno or 1)
