#!/usr/bin/env python3

import pycountry
import os
import subprocess

from pathlib import Path

codes = [country.alpha_2 for country in pycountry.countries]

# For testing
#codes = ["JP", "US", "RU"]

for code in codes:
    output_file = (Path(__file__).parent / f"../data-static/hosting-by-country/{code}.csv").resolve()
    if os.path.exists(output_file):
        print(f"{code} exists, skipping")
    else:
        print(f"Running for {code}")
        script = (Path(__file__).parent / f"../data-fetchers/fetch-hosting-iyp.py").resolve()
        subprocess.run(["python3", script, code])
        

