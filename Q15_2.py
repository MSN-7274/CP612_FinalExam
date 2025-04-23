#!/usr/bin/env python3
"""
Fuzzy Merge-Join with External Sorting
Author: <your name>
"""

import csv
import heapq
import os
import tempfile
from difflib import SequenceMatcher
from pathlib import Path
from typing import List, Tuple

# Parameters
MEM_LIMIT        = 50_000         # lines per run (tune for available RAM)
FUZZY_THRESHOLD  = 0.90           # similarity score required for a match
STUDENT_CSV      = "Student.csv"
UNIV_CSV         = "University.csv"
OUT_CSV          = "Result.csv"

# Helper functions
def normalize(name: str) -> str:
    """Return a lowercase, whitespace-free version of the string."""
    return ''.join(name.lower().split())

def similarity(a: str, b: str) -> float:
    """Compute similarity score between two strings (0.0–1.0)."""
    return SequenceMatcher(None, a, b).ratio()

# External sort: create sorted runs
def create_runs(src: str, key_idx: int) -> Tuple[List[Path], List[str]]:
    runs = []
    with open(src, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)             # keep the header row
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) >= MEM_LIMIT:
                runs.append(write_run(chunk, header, key_idx))
                chunk.clear()
        if chunk:
            runs.append(write_run(chunk, header, key_idx))
    return runs, header

def write_run(rows: List[List[str]], header: List[str], key_idx: int) -> Path:
    rows.sort(key=lambda r: normalize(r[key_idx]))
    fd, path = tempfile.mkstemp(suffix=".csv", dir=".")
    with os.fdopen(fd, 'w', newline='', encoding='utf-8') as out:
        writer = csv.writer(out)
        writer.writerow(header)
        writer.writerows(rows)
    return Path(path)

# Cursor for one run
class RunCursor:
    def __init__(self, path: Path, key_idx: int):
        self.file = open(path, newline='', encoding='utf-8')
        self.reader = csv.reader(self.file)
        next(self.reader)                 # skip header
        self.key_idx = key_idx
        self.row = None
        self._advance()

    def _advance(self):
        try:
            self.row = next(self.reader)
        except StopIteration:
            self.row = None               # end of run

    def key(self) -> str:
        return normalize(self.row[self.key_idx]) if self.row else ''

    def close(self):
        self.file.close()

# Multiway merge-join with fuzzy matching
def merge_join(student_runs: List[Path], univ_runs: List[Path],
               s_key: int, u_key: int, country_idx: int):
    # Build cursors for every run
    s_cur = [RunCursor(p, s_key) for p in student_runs]
    u_cur = [RunCursor(p, u_key) for p in univ_runs]

    with open(OUT_CSV, 'w', newline='', encoding='utf-8') as fout:
        writer = csv.writer(fout)
        writer.writerow(["StudentID", "Name", "Major",
                         "University", "Country"])

        # Min-heaps keyed by normalized name
        s_heap = [(c.key(), i) for i, c in enumerate(s_cur) if c.row]
        u_heap = [(c.key(), i) for i, c in enumerate(u_cur) if c.row]
        heapq.heapify(s_heap)
        heapq.heapify(u_heap)

        while s_heap and u_heap:
            s_key_norm, si = s_heap[0]
            u_key_norm, ui = u_heap[0]
            score = similarity(s_key_norm, u_key_norm)

            if score >= FUZZY_THRESHOLD:
                # Collect all rows with the same normalized key from both sides
                s_rows = pop_same_keys(s_heap, s_cur, s_key_norm)
                u_rows = pop_same_keys(u_heap, u_cur, u_key_norm)
                for srow in s_rows:
                    for urow in u_rows:
                        if urow[country_idx].lower() == 'canada':
                            writer.writerow(srow + [urow[country_idx]])
            else:
                # Advance the side with the smaller key (lexicographically)
                if s_key_norm < u_key_norm:
                    advance_heap(s_heap, s_cur)
                else:
                    advance_heap(u_heap, u_cur)

    # Close files
    for c in s_cur + u_cur:
        c.close()

def pop_same_keys(heap, cursors, key_norm):
    """Return and advance all rows whose normalized key equals key_norm."""
    rows = []
    while heap and heap[0][0] == key_norm:
        _, idx = heapq.heappop(heap)
        rows.append(cursors[idx].row)
        cursors[idx]._advance()
        if cursors[idx].row:
            heapq.heappush(heap, (cursors[idx].key(), idx))
    return rows

def advance_heap(heap, cursors):
    """Pop top entry, advance its cursor, and push back if not exhausted."""
    _, idx = heapq.heappop(heap)
    cursors[idx]._advance()
    if cursors[idx].row:
        heapq.heappush(heap, (cursors[idx].key(), idx))

def main():
    # Column indices
    # Student: StudentID, Name, Major, University, ...
    s_univ_idx = 3
    # University: UniversityName, Country
    u_name_idx, u_country_idx = 0, 1

    # 1) External sort: generate runs
    student_runs, _ = create_runs(STUDENT_CSV, s_univ_idx)
    univ_runs, _    = create_runs(UNIV_CSV,  u_name_idx)

    # 2) Merge-join with fuzzy matching
    merge_join(student_runs, univ_runs,
               s_univ_idx, u_name_idx, u_country_idx)

    # 3) Cleanup temporary run files
    for p in student_runs + univ_runs:
        p.unlink()

if __name__ == "__main__":
    main()
