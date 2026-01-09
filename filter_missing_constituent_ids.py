"""Filter out constituent IDs that are missing from the database."""

from pathlib import Path
from typing import List, Sequence, Set

import psycopg2

from db_config import load_db_config

INPUT_FILE = Path(__file__).with_name("constituent_duplicate_ids_from_list.txt")
MISSING_FILE = Path(__file__).with_name("constituent_missing_ids.txt")


def read_id_lines(path: Path) -> List[List[int]]:
    """Read comma-separated ID lines from the given file."""
    id_lines: List[List[int]] = []
    with path.open("r", encoding="utf-8") as handle:
        for idx, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                id_lines.append([])
                continue

            ids: List[int] = []
            for part in line.split(","):
                part = part.strip()
                if not part:
                    continue
                try:
                    ids.append(int(part))
                except ValueError as exc:
                    raise ValueError(f"Line {idx}: invalid constituent id {part!r}") from exc
            id_lines.append(ids)
    return id_lines


def fetch_existing_ids(ids: Sequence[int]) -> Set[int]:
    """Return IDs that exist in the CONSTITUENT table."""
    if not ids:
        return set()

    unique_ids = list({int(cid) for cid in ids})

    db_config = load_db_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "CONSTITUENT_ID" FROM "CONSTITUENT" '
                'WHERE "CONSTITUENT_ID" = ANY(%s)',
                (unique_ids,),
            )
            rows = cur.fetchall()

    return {cid for (cid,) in rows}


def write_filtered_ids(id_lines: Sequence[Sequence[int]], existing: Set[int], path: Path) -> int:
    """Rewrite file keeping only IDs present in the database. Returns number removed."""
    removed = 0
    with path.open("w", encoding="utf-8") as handle:
        for ids in id_lines:
            kept_ids = [str(cid) for cid in ids if cid in existing]
            removed += len(ids) - len(kept_ids)
            handle.write(",".join(kept_ids))
            handle.write("\n")
    return removed


def write_missing_ids(missing: Sequence[int], path: Path) -> None:
    """Write missing IDs to a helper file for reference."""
    with path.open("w", encoding="utf-8") as handle:
        for cid in missing:
            handle.write(f"{cid}\n")


def main() -> None:
    id_lines = read_id_lines(INPUT_FILE)
    all_ids = [cid for line in id_lines for cid in line]
    existing_ids = fetch_existing_ids(all_ids)
    missing_ids = sorted(set(all_ids) - existing_ids)

    removed_count = write_filtered_ids(id_lines, existing_ids, INPUT_FILE)
    write_missing_ids(missing_ids, MISSING_FILE)

    print(f"Total IDs in file: {len(all_ids)}")
    print(f"Found in DB: {len(existing_ids)}")
    print(f"Removed (missing): {removed_count}")
    print(f"Missing IDs written to: {MISSING_FILE}")


if __name__ == "__main__":
    main()

