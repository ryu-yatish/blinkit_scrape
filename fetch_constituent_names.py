"""Generate name lines for constituent IDs listed in constituent_duplicate_ids_from_list.txt."""

from pathlib import Path
from typing import Dict, List, Sequence, Set

import psycopg2

from db_config import load_db_config

INPUT_FILE = Path(__file__).with_name("constituent_duplicate_ids_from_list.txt")
OUTPUT_FILE = Path(__file__).with_name("constituent_duplicate_names_from_db.txt")
PLACEHOLDER_PREFIX = "MISSING_ID_"


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


def fetch_names_for_ids(ids: Sequence[int]) -> Dict[int, str]:
    """Return a mapping of constituent_id -> name for the provided IDs."""
    if not ids:
        return {}

    unique_ids = list({int(cid) for cid in ids})

    db_config = load_db_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                'SELECT "CONSTITUENT_ID", "NAME" FROM "CONSTITUENT" '
                'WHERE "CONSTITUENT_ID" = ANY(%s)',
                (unique_ids,),
            )
            rows = cur.fetchall()

    return {cid: name for cid, name in rows}


def write_names_file(id_lines: Sequence[Sequence[int]], id_to_name: Dict[int, str], path: Path) -> Set[int]:
    """Write comma-separated names per line, preserving the input structure."""
    missing: Set[int] = set()

    with path.open("w", encoding="utf-8") as handle:
        for ids in id_lines:
            names: List[str] = []
            for cid in ids:
                name = id_to_name.get(cid)
                if name is None:
                    name = f"{PLACEHOLDER_PREFIX}{cid}"
                    missing.add(cid)
                names.append(name)

            handle.write(",".join(names))
            handle.write("\n")

    return missing


def main() -> None:
    id_lines = read_id_lines(INPUT_FILE)
    flat_ids = [cid for line in id_lines for cid in line]
    id_to_name = fetch_names_for_ids(flat_ids)
    missing = write_names_file(id_lines, id_to_name, OUTPUT_FILE)

    print(f"Wrote names for {len(id_to_name)} constituents to {OUTPUT_FILE}")
    if missing:
        preview = ", ".join(map(str, sorted(missing)[:10]))
        suffix = "..." if len(missing) > 10 else ""
        print(f"{len(missing)} IDs were missing in the DB: {preview}{suffix}")
    else:
        print("All IDs were resolved to names.")


if __name__ == "__main__":
    main()

