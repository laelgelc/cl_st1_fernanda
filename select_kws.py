#!/usr/bin/env python3
import os
import glob
import unicodedata

# --- Configuration ---
INPUT_DIR    = "corpus/10_keywords"
MAX_KEYWORDS = 200

# Compute OUTPUT_DIR as a sibling of INPUT_DIR (not inside it)
parent = os.path.dirname(os.path.abspath(INPUT_DIR))
OUTPUT_DIR = os.path.join(parent, "11_kw_selected")

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def contains_punctuation(s):
    # True if any character in s is in a Unicode punctuation category
    return any(unicodedata.category(ch).startswith("P") for ch in s)

# Set to hold all unique keywords
consolidated = set()

# Process each keyword file
for filepath in glob.glob(os.path.join(INPUT_DIR, "*.txt")):
    filename = os.path.basename(filepath)
    outpath = os.path.join(OUTPUT_DIR, filename)
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
            if len(selected) >= MAX_KEYWORDS:
                break

    # Write the selected lemmas to the individual output file
    with open(outpath, "w", encoding="utf-8") as fout:
        for lemma in selected:
            fout.write(f"{lemma}\n")

# Write consolidated keywords (unique) to keywords.txt
consolidated_path = os.path.join(OUTPUT_DIR, "keywords.txt")
with open(consolidated_path, "w", encoding="utf-8") as fout:
    for lemma in sorted(consolidated):
        fout.write(f"{lemma}\n")

print(f"Top {MAX_KEYWORDS} POSKW keywords (no punctuation, digits, or uppercase) selected and written to: {OUTPUT_DIR}")
print(f"Consolidated keywords written to: {consolidated_path}")
