#!/bin/bash
# pipeline_daily_run.sh - 每日 15:15 收盘后执行数据更新
source /home/claire/projects/qrp-atlas/.venv/bin/activate
cd /home/claire/projects/qrp-atlas
python3 -c "
import sys, traceback
from qrp_atlas.pipeline.daily_update.run import run

try:
    run()
    print('PIPELINE_EXIT=0')
except Exception as e:
    print(f'PIPELINE_EXIT=1')
    print(f'ERROR: {e}')
    traceback.print_exc()
" 2>&1
