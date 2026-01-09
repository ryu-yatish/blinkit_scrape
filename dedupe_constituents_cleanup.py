"""Consolidate duplicate constituents by keeping the first ID on each line.

For every comma-separated line in `constituent_duplicate_ids_from_list.txt`:
- Keep the first constituent ID.
- Re-point `PRODUCT_CONSTITUENT` rows that reference any other IDs in the line
  to the first ID.
- Delete `CONSTITUENT_GOAL_WEIGHTS` rows for the duplicate IDs.
- Delete the duplicate constituent rows themselves.

Only lines containing two or more IDs are processed. All operations run in a
single transaction so a failure will roll everything back.
"""

from pathlib import Path
from typing import List, Sequence, Set, Tuple

import psycopg2

from db_config import load_db_config

INPUT_FILE = Path(__file__).with_name("constituent_duplicate_ids_from_list.txt")


def read_id_groups(path: Path) -> List[List[int]]:
    """Read comma-separated ID lines from the given file."""
    groups: List[List[int]] = []
    with path.open("r", encoding="utf-8") as handle:
        for idx, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                groups.append([])
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
            groups.append(ids)
    return groups


def fetch_existing_ids(conn, ids: Sequence[int]) -> Set[int]:
    """Return IDs that exist in the CONSTITUENT table."""
    if not ids:
        return set()

    unique_ids = list({int(cid) for cid in ids})
    with conn.cursor() as cur:
        cur.execute(
            'SELECT "CONSTITUENT_ID" FROM "CONSTITUENT" WHERE "CONSTITUENT_ID" = ANY(%s)',
            (unique_ids,),
        )
        rows = cur.fetchall()
    return {cid for (cid,) in rows}


def update_product_constituents(conn, primary_id: int, duplicate_ids: Sequence[int]) -> int:
    """Point PRODUCT_CONSTITUENT rows from duplicates to the primary."""
    if not duplicate_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            'UPDATE "PRODUCT_CONSTITUENT" '
            'SET "CONSTITUENT_ID" = %s '
            'WHERE "CONSTITUENT_ID" = ANY(%s)',
            (primary_id, list(duplicate_ids)),
        )
        return cur.rowcount


def delete_goal_weights(conn, duplicate_ids: Sequence[int]) -> int:
    """Delete CONSTITUENT_GOAL_WEIGHTS rows for the duplicate IDs."""
    if not duplicate_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            'DELETE FROM "CONSTITUENT_GOAL_WEIGHTS" WHERE "CONSTITUENT_ID" = ANY(%s)',
            (list(duplicate_ids),),
        )
        return cur.rowcount


def delete_constituents(conn, duplicate_ids: Sequence[int]) -> int:
    """Delete duplicate rows from CONSTITUENT."""
    if not duplicate_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            'DELETE FROM "CONSTITUENT" WHERE "CONSTITUENT_ID" = ANY(%s)',
            (list(duplicate_ids),),
        )
        return cur.rowcount


def constituent_exists(conn, constituent_id: int) -> bool:
    """Check if a constituent exists (row presence only, regardless of deleted flag)."""
    with conn.cursor() as cur:
        cur.execute(
            'SELECT 1 FROM "CONSTITUENT" WHERE "CONSTITUENT_ID" = %s',
            (constituent_id,),
        )
        return cur.fetchone() is not None


def normalize_group(ids: Sequence[int]) -> List[int]:
    """Remove duplicate IDs while preserving original order."""
    seen: Set[int] = set()
    normalized: List[int] = []
    for cid in ids:
        if cid in seen:
            continue
        seen.add(cid)
        normalized.append(cid)
    return normalized


def process_group(conn, primary_id: int, duplicate_ids: Sequence[int]) -> Tuple[int, int, int]:
    """Process one group and return (product_updates, goal_deletes, constituent_deletes)."""
    updated_products = update_product_constituents(conn, primary_id, duplicate_ids)
    deleted_goals = delete_goal_weights(conn, duplicate_ids)
    deleted_constituents = delete_constituents(conn, duplicate_ids)
    return updated_products, deleted_goals, deleted_constituents


def main() -> None:
    groups = [normalize_group(ids) for ids in read_id_groups(INPUT_FILE)]
    actionable = [group for group in groups if len(group) >= 2]

    all_ids = [cid for group in actionable for cid in group]
    protected_primary_ids = {group[0] for group in actionable}

    db_config = load_db_config()
    with psycopg2.connect(**db_config) as conn:
        existing_ids = fetch_existing_ids(conn, all_ids)

        total_product_updates = 0
        total_goal_deletes = 0
        total_constituent_deletes = 0
        skipped_groups = 0

        for idx, group in enumerate(actionable, start=1):
            primary_id = group[0]
            duplicate_ids = [
                cid
                for cid in group[1:]
                if cid != primary_id and cid not in protected_primary_ids
            ]
            if not duplicate_ids:
                continue

            if primary_id not in existing_ids or not constituent_exists(conn, primary_id):
                print(
                    f"Skipping group {idx}: primary constituent {primary_id} is missing in the database."
                )
                skipped_groups += 1
                continue

            filtered_out = set(group[1:]) - set(duplicate_ids)
            if filtered_out:
                print(
                    f"Group {idx}: skipped {sorted(filtered_out)} because they are primary in another group."
                )

            product_updates, goal_deletes, constituent_deletes = process_group(
                conn, primary_id, duplicate_ids
            )

            total_product_updates += product_updates
            total_goal_deletes += goal_deletes
            total_constituent_deletes += constituent_deletes

            print(
                f"Group {idx}: kept {primary_id}, "
                f"re-pointed {product_updates} product rows, "
                f"deleted {goal_deletes} goal weights, "
                f"deleted {constituent_deletes} duplicate constituents ({duplicate_ids})."
            )

        print("\nSummary")
        print(f"Groups processed: {len(actionable) - skipped_groups} (skipped {skipped_groups})")
        print(f"Product rows re-pointed: {total_product_updates}")
        print(f"Goal weight rows deleted: {total_goal_deletes}")
        print(f"Constituent rows deleted: {total_constituent_deletes}")


if __name__ == "__main__":
    main()

