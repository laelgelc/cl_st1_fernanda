#!/usr/bin/env python3
# Prerequisite: Update `keywords.env`
# Usage: python select_kws.py
from dotenv import load_dotenv
from pathlib import Path
import os

# Load environment variables
load_dotenv(dotenv_path="keywords.env")
base_dir = os.getenv("BASE_DIR")
keywords_dir = os.getenv("KEYWORDS_DIR")
keywords_path = os.path.join(base_dir, keywords_dir)
selected_keywords_dir = os.getenv("SELECTED_KEYWORDS_DIR")
selected_keywords_path = os.path.join(base_dir, selected_keywords_dir)
max_keywords = int(os.getenv("MAX_KEYWORDS"))

# Ensure the output directory exists
os.makedirs(selected_keywords_path, exist_ok=True)

# Create `selectedwords` from `keywords.txt`
consolidated_path = os.path.join(selected_keywords_path, "keywords.txt")
inp = Path(consolidated_path)
outp = Path.cwd() / "selectedwords"

lines = [ln.rstrip("\n") for ln in inp.read_text(encoding="utf-8").splitlines() if ln.strip()]
width = 6
with outp.open("w", encoding="utf-8") as f:
    for i, text in enumerate(lines, start=1):
        f.write(f"v{str(i).zfill(width)} {text}\n")

# Make a copy named `var_index.txt` in the same directory as `selectedwords`
var_index_path = outp.with_name("var_index.txt")
var_index_path.write_text(outp.read_text(encoding="utf-8"), encoding="utf-8")

print("selectedwords and var_index.txt created from keywords.txt")
