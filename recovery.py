"""Durable study checkpoints. The manifest always points to a complete PWB."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from uuid import uuid4

from .powerworld import PowerWorldError


class StopRequested(Exception):
    """A cooperative stop, raised only outside a running PowerWorld call."""


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def contingency_table(client) -> list[tuple[str, str, str, str]]:
    result = client.get_parameters_multiple(
        "Contingency", ("CTGLabel", "CTGSkip", "CTGViol", "CTGSolved"), ""
    )
    try:
        columns = result[1]
        if len(columns) != 4:
            raise ValueError("Expected four columns")
        columns = [list(column) if column is not None else [] for column in columns]
        if len({len(column) for column in columns}) != 1:
            raise ValueError("Column lengths differ")
        rows = [tuple("" if value is None else str(value).strip() for value in row)
                for row in zip(*columns)]
        if len({row[0] for row in rows}) != len(rows) or any(not row[0] for row in rows):
            raise ValueError("Missing or duplicate contingency labels")
        if any(row[1].upper() not in {"YES", "NO"} for row in rows):
            raise ValueError("Unexpected skip flag")
        return rows
    except (IndexError, TypeError, ValueError) as exc:
        raise PowerWorldError(f"Invalid contingency table: {exc}") from exc


def table_digest(rows) -> str:
    # The order may change when PowerWorld reloads a case; identities must not.
    return hashlib.sha256(json.dumps(sorted(rows), ensure_ascii=False).encode("utf-8")).hexdigest()


class RecoveryStore:
    """Two alternating snapshots; interrupted saves cannot replace the last good one."""

    def __init__(self, manifest: Path) -> None:
        self.path = Path(manifest)
        self.data: dict = {}

    def write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(self.path.name + ".tmp")
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(self.data, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(self.path)

    def commit(self, client, state: dict) -> None:
        slot = 1 - int(self.data.get("slot", 1))
        run_id = self.data.get("run_id") or uuid4().hex[:12]
        case = self.path.with_name(f"{self.path.stem}_{run_id}_{slot}.PWB")
        # PWB result retention must be explicit, independent of input AUX settings.
        client.change_parameters_single(
            "Ctg_Options", ("CTGSaveInPWB", "CTG_AlwaysSaveResults"), ("YES", "YES")
        )
        rows = contingency_table(client)
        client.save_case(case, "PWB", True)
        if not case.is_file() or case.stat().st_size == 0:
            raise PowerWorldError(f"PowerWorld did not write a usable checkpoint: {case}")
        previous = self.data
        self.data = {
            **state, "schema": 1, "run_id": run_id, "slot": slot,
            "checkpoint": case.name, "sha256": file_digest(case),
            "table_sha256": table_digest(rows),
        }
        try:
            self.write()
        except Exception:
            self.data = previous
            raise

    def load(self) -> dict:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data["schema"] != 1 or data["phase"] not in {"dc", "ac", "full_ac", "outputs", "complete"}:
                raise ValueError("Unsupported recovery format")
            name = data["checkpoint"]
            if Path(name).name != name or not name.lower().endswith(".pwb"):
                raise ValueError("Invalid checkpoint filename")
            if data["sha256"] != file_digest(self.path.parent / name):
                raise ValueError("Checkpoint is incomplete or has changed")
            self.data = data
            return data
        except (OSError, KeyError, TypeError, ValueError) as exc:
            raise PowerWorldError(f"Cannot resume {self.path}: {exc}") from exc

    @property
    def checkpoint(self) -> Path:
        return self.path.parent / self.data["checkpoint"]

    def validate_open_case(self, client) -> None:
        if table_digest(contingency_table(client)) != self.data["table_sha256"]:
            raise PowerWorldError(
                "The reopened checkpoint does not contain the saved contingency states/results. "
                "Stopped before solving so completed work is not silently lost."
            )

    def cleanup_completed(self) -> tuple[Path, ...]:
        """Remove recovery-only files after final outputs are safely published.

        The final PWB/AUX/CSV outputs and the session logfile are deliberately
        not touched.  A failed or stopped run never calls this method, so its
        checkpoint remains available for Resume.
        """
        names: set[Path] = {self.path, self.path.with_name(self.path.name + ".tmp")}
        run_id = str(self.data.get("run_id", "")).strip()
        if run_id:
            for slot in (0, 1):
                names.add(self.path.with_name(f"{self.path.stem}_{run_id}_{slot}.PWB"))
        names.add(self.path.with_suffix(".workers.aux"))
        removed: list[Path] = []
        for path in names:
            try:
                if path.is_file():
                    path.unlink()
                    removed.append(path)
            except OSError:
                # Cleanup is best effort; completed outputs must still be kept.
                continue
        return tuple(removed)
