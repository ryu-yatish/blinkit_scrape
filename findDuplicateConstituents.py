"""
Identify and group duplicate constituents from the CONSTITUENT table.

Steps:
- Fetch all constituents from Postgres (sorted alphabetically).
- Use a sliding window similarity scan to propose candidate duplicate clusters.
- Ask GPT to confirm which candidates are true duplicates.
- Write each confirmed duplicate cluster as comma-separated IDs to an output file.

The script intentionally keeps similarity heuristics generous so GPT can make the
final call. No database updates are performed.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

try:
    import psycopg2
except ImportError as exc:  # pragma: no cover - handled in main guard
    psycopg2 = None  # type: ignore
    _PSYCOPG2_IMPORT_ERROR = exc
else:
    _PSYCOPG2_IMPORT_ERROR = None

try:
    from openai import OpenAI

    _OPENAI_CLIENT_CLASS = OpenAI
    _OPENAI_IS_V1 = True
except Exception:
    _OPENAI_CLIENT_CLASS = None
    _OPENAI_IS_V1 = False

try:
    import openai  # type: ignore
except Exception:
    openai = None  # type: ignore

from db_config import load_db_config


NON_ALNUM = re.compile(r"[^a-z0-9]+")
TOKEN_SPLIT = re.compile(r"[^a-z0-9]+")
CODE_NUMBER = re.compile(r"(?:ins|e)?\s*(-?\d{2,4})")


@dataclass
class Constituent:
    constituent_id: int
    name: str
    normalized: str
    compact: str
    tokens: Set[str]
    codes: Set[str]


def normalize_name(name: str) -> Tuple[str, str, Set[str], Set[str]]:
    lower = name.lower()
    normalized = NON_ALNUM.sub(" ", lower).strip()
    compact = NON_ALNUM.sub("", lower)
    tokens = {t for t in TOKEN_SPLIT.split(lower) if t}
    codes = set(CODE_NUMBER.findall(lower))
    return normalized, compact, tokens, codes


def build_constituents(rows: Sequence[Tuple[int, str]]) -> List[Constituent]:
    result: List[Constituent] = []
    for cid, name in rows:
        normalized, compact, tokens, codes = normalize_name(name or "")
        result.append(
            Constituent(
                constituent_id=cid,
                name=name or "",
                normalized=normalized,
                compact=compact,
                tokens=tokens,
                codes=codes,
            )
        )
    return result


def similarity_score(a: Constituent, b: Constituent) -> float:
    return SequenceMatcher(None, a.compact, b.compact).ratio()


def likely_duplicate(a: Constituent, b: Constituent) -> bool:
    sim = similarity_score(a, b)
    shared_tokens = a.tokens & b.tokens
    shared_codes = a.codes & b.codes

    if sim >= 0.86:
        return True
    if shared_codes and sim >= 0.7:
        return True
    if len(shared_tokens) >= 2 and sim >= 0.72:
        return True
    if shared_tokens and shared_codes:
        return True
    if sim >= 0.78 and (shared_tokens or shared_codes):
        return True
    return False


class UnionFind:
    def __init__(self, size: int):
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x: int, y: int) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self.rank[rx] < self.rank[ry]:
            self.parent[rx] = ry
        elif self.rank[rx] > self.rank[ry]:
            self.parent[ry] = rx
        else:
            self.parent[ry] = rx
            self.rank[rx] += 1


def cluster_candidates(constituents: List[Constituent], window: int = 30) -> List[List[Constituent]]:
    uf = UnionFind(len(constituents))
    for i, a in enumerate(constituents):
        upper = min(len(constituents), i + window + 1)
        for j in range(i + 1, upper):
            b = constituents[j]
            if likely_duplicate(a, b):
                uf.union(i, j)

    clusters: Dict[int, List[Constituent]] = defaultdict(list)
    for idx, c in enumerate(constituents):
        clusters[uf.find(idx)].append(c)

    return [sorted(cluster, key=lambda c: c.name.lower()) for cluster in clusters.values() if len(cluster) > 1]


def init_openai_client(api_key: str):
    if _OPENAI_IS_V1 and _OPENAI_CLIENT_CLASS:
        return _OPENAI_CLIENT_CLASS(api_key=api_key)
    if openai is None:  # pragma: no cover - dependency check
        raise RuntimeError("openai package is not installed. pip install openai")
    openai.api_key = api_key
    return openai


def call_gpt(client, model: str, cluster: List[Constituent]) -> List[List[int]]:
    records_text = "\n".join(f"{c.constituent_id}: {c.name}" for c in cluster)
    system_msg = (
        "You are a data cleanup assistant. "
        "Given a list of food additive constituents with IDs and names, "
        "identify which entries refer to the SAME substance. "
        "Treat differences in casing, hyphens, spacing, punctuation, and INS/E numbers as formatting only. "
        "Only use general knowledge about additive numbers and obvious synonyms; do not invent substances."
    )
    user_msg = (
        "Records:\n"
        f"{records_text}\n\n"
        "Return only JSON with a key `duplicate_groups`, which is a list of lists of IDs. "
        "Each inner list must contain IDs that are duplicates of each other (same substance). "
        "Do not include groups of size 1. Do not include explanations."
    )

    if _OPENAI_IS_V1 and _OPENAI_CLIENT_CLASS:
        resp = client.chat.completions.create(
            model=model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
        )
        content = resp.choices[0].message.content
    else:
        resp = client.ChatCompletion.create(
            model=model,
            temperature=0,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
        )
        content = resp["choices"][0]["message"]["content"]

    return parse_duplicate_groups(content)


def parse_duplicate_groups(text: str) -> List[List[int]]:
    try:
        json_str = extract_json_block(text)
        data = json.loads(json_str)
        groups = data.get("duplicate_groups", [])
        cleaned: List[List[int]] = []
        for group in groups:
            try:
                ids = sorted({int(x) for x in group})
                if len(ids) > 1:
                    cleaned.append(ids)
            except Exception:
                continue
        return cleaned
    except Exception:
        logging.exception("Failed to parse GPT response: %s", text)
        return []


def extract_json_block(text: str) -> str:
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError("No JSON block found")
    return match.group(0)


def dedupe_groups(groups: Iterable[Iterable[int]]) -> List[List[int]]:
    unique: Set[Tuple[int, ...]] = set()
    for group in groups:
        ordered = tuple(sorted(set(group)))
        if len(ordered) > 1:
            unique.add(ordered)
    return [list(g) for g in sorted(unique)]


def fetch_constituents(conn) -> List[Tuple[int, str]]:
    query = 'SELECT "CONSTITUENT_ID", "NAME" FROM "CONSTITUENT" WHERE NOT "DELETED" ORDER BY "NAME" ASC;'
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def load_from_db(db_config: Dict[str, Any]) -> List[Constituent]:
    if psycopg2 is None:
        raise RuntimeError(f"psycopg2 is missing: {_PSYCOPG2_IMPORT_ERROR}")

    conn = psycopg2.connect(connect_timeout=10, **db_config)
    try:
        rows = fetch_constituents(conn)
    finally:
        conn.close()
    return build_constituents(rows)


def write_output(groups: Sequence[Sequence[int]], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for group in groups:
            f.write(",".join(str(x) for x in group) + "\n")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Find duplicate constituents and emit duplicate ID groups.")
    parser.add_argument("--host", default=os.environ.get("PGHOST"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 5432)))
    parser.add_argument("--user", default=os.environ.get("PGUSER"))
    parser.add_argument("--password", default=os.environ.get("PGPASSWORD"))
    parser.add_argument("--database", default=os.environ.get("PGDATABASE"))
    parser.add_argument("--window-size", type=int, default=35, help="Sliding window size for similarity scan.")
    parser.add_argument("--model", default="gpt-4o-mini", help="OpenAI model to use for verification.")
    parser.add_argument(
        "--openai-key",
        default=os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_KEY"),
        help="OpenAI API key (or set OPENAI_API_KEY env).",
    )
    parser.add_argument(
        "--output",
        default="constituent_duplicate_ids_from_list.txt",
        help="Path to write duplicate ID groups (CSV per line).",
    )
    parser.add_argument(
        "--candidates-output",
        default="candidate_clusters.json",
        help="Optional JSON dump of heuristic clusters before GPT review.",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING...).")
    return parser.parse_args(argv)


def build_db_config(args: argparse.Namespace) -> Dict[str, Any]:
    overrides = {
        "host": args.host,
        "port": args.port,
        "dbname": args.database,
        "user": args.user,
        "password": args.password,
    }
    return load_db_config(overrides)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if not args.openai_key:
        logging.error("OpenAI API key missing. Set OPENAI_API_KEY env or pass --openai-key.")
        return 1

    try:
        db_config = build_db_config(args)
    except RuntimeError as exc:
        logging.error("%s", exc)
        return 1

    constituents = load_from_db(db_config)
    logging.info("Fetched %d constituents", len(constituents))

    clusters = cluster_candidates(constituents, window=args.window_size)
    logging.info("Identified %d candidate clusters for GPT review", len(clusters))

    with open(args.candidates_output, "w", encoding="utf-8") as f:
        json.dump(
            [
                {"ids": [c.constituent_id for c in cluster], "names": [c.name for c in cluster]}
                for cluster in clusters
            ],
            f,
            indent=2,
        )

    client = init_openai_client(args.openai_key)

    all_groups: List[List[int]] = []
    for cluster in clusters:
        groups = call_gpt(client, args.model, cluster)
        if groups:
            all_groups.extend(groups)

    deduped_groups = dedupe_groups(all_groups)
    write_output(deduped_groups, args.output)
    logging.info("Wrote %d duplicate groups to %s", len(deduped_groups), args.output)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())

