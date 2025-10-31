#!/usr/bin/env python3
# Prerequisite: Update `keywords.env`
# Usage: python select_kws.py
from dotenv import load_dotenv
from pathlib import Path
import os
import glob
import unicodedata

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

def contains_punctuation(s):
    # True if any character in s is in a Unicode punctuation category
    return any(unicodedata.category(ch).startswith("P") for ch in s)

# Set to hold all unique keywords
consolidated = set()

# Process each keyword file
for filepath in glob.glob(os.path.join(keywords_path, "*.txt")):
    filename = os.path.basename(filepath)
    outpath = os.path.join(selected_keywords_path, filename)
    selected = []

    # Read and skip the header
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()[1:]  # drop the first line (header)

    # Filter for POSKW, ignore any lemma containing punctuation, digits, or uppercase letters
    for line in lines:
        parts = line.strip().split()
        if len(parts) < 2:
            continue
        lemma, status = parts[0], parts[-1]
        # skip lemmas with punctuation, numbers, or uppercase letters
        if (contains_punctuation(lemma) or
            any(ch.isdigit() for ch in lemma) or
            any(ch.isupper() for ch in lemma)):
            continue
        if status == "POSKW":
            selected.append(lemma)
            consolidated.add(lemma)
            if len(selected) >= max_keywords:
                break

    # Write the selected lemmas to the individual output file
    with open(outpath, "w", encoding="utf-8") as fout:
        for lemma in selected:
            fout.write(f"{lemma}\n")

# Write consolidated keywords (unique) to keywords.txt
consolidated_path = os.path.join(selected_keywords_path, "keywords.txt")
with open(consolidated_path, "w", encoding="utf-8") as fout:
    for lemma in sorted(consolidated):
        fout.write(f"{lemma}\n")

print(f"Top {max_keywords} POSKW keywords (no punctuation, digits, or uppercase) selected and written to: {selected_keywords_path}")
print(f"Consolidated keywords written to: {consolidated_path}")
