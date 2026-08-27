#!/usr/bin/env python3
# Prerequisite: Update `keywords.env`
# Usage: python keywords_text_counts.py
from dotenv import load_dotenv
import os
import glob
import math
import multiprocessing
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Load environment variables
load_dotenv(dotenv_path="keywords.env")
base_dir = os.getenv("BASE_DIR")
lemma_tokens_dir = os.getenv("LEMMA_TOKENS_DIR")
lemma_tokens_path = os.path.join(base_dir, lemma_tokens_dir)
keywords_dir = os.getenv("KEYWORDS_DIR")
keywords_path = os.path.join(base_dir, keywords_dir)
log_likelihood_threshold = float(os.getenv("LOG_LIKELIHOOD_THRESHOLD"))
label_prefix = os.getenv("LABEL_PREFIX")

def LL(a, b, c, d):
    """Compute log‑likelihood for a 2×2 frequency table."""
    if a == 0 or b == 0:
        return 0.0
    e1 = c * (a + b) / (c + d)
    e2 = d * (a + b) / (c + d)
    return 2 * (a * math.log(a / e1) + b * math.log(b / e2))

def process_label(args):
    """Compute and save keywords for one label."""
    label_name, label_counts, tok_label, global_counts, total_tokens = args
    d_tok = total_tokens - tok_label
    rows = []
    for lemma, glob_count in global_counts.items():
        a = label_counts.get(lemma, 0)
        b = glob_count - a
        perA = (a / tok_label) * 1000 if tok_label else 0.0
        perB = (b / d_tok) * 1000 if d_tok else 0.0
        expected = (tok_label * (a + b)) / (tok_label + d_tok) if (tok_label + d_tok) else 0.0
        ll_stat = LL(a, b, tok_label, d_tok)
        diff = (100 * (perA - perB) / ((perA + perB) / 2)) if (perA + perB) else 0.0
        status = (
            "POSKW" if ll_stat >= log_likelihood_threshold and diff > 0 else
            "NEGKW" if ll_stat >= log_likelihood_threshold else
            "NOTKW"
        )
        rows.append((lemma, a, b,
                     round(perA, 2), round(perB, 2),
                     round(expected, 2),
                     round(ll_stat, 2),
                     round(diff, 2),
                     status))

    # Sort: POSKW first, then by LL descending
    priority = {"POSKW": 0, "NEGKW": 1, "NOTKW": 2}
    rows.sort(key=lambda r: (priority[r[8]], -r[6]))

    # Write out
    os.makedirs(keywords_path, exist_ok=True)
    out_label = f"{label_prefix}{label_name}" if label_prefix else label_name
    outpath = os.path.join(keywords_path, f"{out_label}.txt")
    with open(outpath, "w", encoding="utf-8") as fout:
        fout.write("lemma target_count comparison_count "
                   "target_per_1k comparison_per_1k expected LL %DIFF status\n")
        for r in rows:
            fout.write(" ".join(map(str, r)) + "\n")
    return label_name

def main():
    # 1) Find labeled subfolders
    try:
        all_entries = sorted(os.listdir(lemma_tokens_path))
    except FileNotFoundError:
        print(f"Error: '{lemma_tokens_path}' not found.")
        return

    # Accept only directories
    all_dirs = [d for d in all_entries if os.path.isdir(os.path.join(lemma_tokens_path, d))]

    if label_prefix:
        prefixed_dirs = [d for d in all_dirs if d.startswith(label_prefix)]
        if not prefixed_dirs:
            print(f"No '{label_prefix}*' subfolders found in {lemma_tokens_path}.")
            return
        # Map raw folder to clean label by stripping the prefix
        mapping = {d: d[len(label_prefix):] for d in prefixed_dirs}
    else:
        # No prefix: use every directory and its name as the label
        if not all_dirs:
            print(f"No subfolders found in {lemma_tokens_path}.")
            return
        mapping = {d: d for d in all_dirs}

    # 2) Load counts
    counters = {}
    tok = {}
    global_counts = Counter()

    for raw_dir, label_name in mapping.items():
        c = Counter()
        total = 0
        folder = os.path.join(lemma_tokens_path, raw_dir)
        for filepath in glob.glob(os.path.join(folder, "*.txt")):
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    lemma = line.strip()
                    if lemma:
                        c[lemma] += 1
                        global_counts[lemma] += 1
                        total += 1
        counters[label_name] = c
        tok[label_name] = total
        print(f"Loaded {total} tokens for '{label_name}'")

    total_tokens = sum(tok.values())
    if total_tokens == 0:
        print("No tokens found across labels. Nothing to do.")
        return

    print(f"Global token count: {total_tokens}\n")

    # 3) Prepare tasks
    tasks = [
        (label, counters[label], tok[label], global_counts, total_tokens)
        for label in counters
    ]

    # 4) Parallel processing
    n_workers = max(1, multiprocessing.cpu_count() - 1)
    print(f"Running keyword extraction on {len(tasks)} labels "
          f"with {n_workers} workers...\n")

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(process_label, t): t[0] for t in tasks}
        with tqdm(total=len(tasks), desc="Keyword extraction", unit="label") as pbar:
            for future in as_completed(futures):
                label = futures[future]
                try:
                    result = future.result()
                    tqdm.write(f"Keywords saved for '{result}'")
                except Exception as e:
                    tqdm.write(f"ERROR on {label}: {e}")
                finally:
                    pbar.update(1)

    print(f"\nDone! Keyword files are in '{keywords_path}/'")

if __name__ == "__main__":
    main()
