import os
import subprocess
import sys

from envfile import load_env

load_env()
dbt_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dbt")
sys.exit(subprocess.call(["dbt", *sys.argv[1:], "--profiles-dir", "."], cwd=dbt_dir))