"""PowerWorld executable and SimAuto integration boundary.

Importing this module does not require PowerWorld or pywin32.  That keeps the
GUI and unit tests usable on machines where Simulator is not installed.
"""

from __future__ import annotations

import gc
from pathlib import Path
import subprocess
from typing import Any, Callable, Sequence


class PowerWorldError(RuntimeError):
    """Raised when PowerWorld cannot be found or rejects an operation."""


def powerworld_executable_candidates(version: int) -> tuple[Path, ...]:
    """Return the install paths used by the legacy application."""
    names = (f"Simulator{version}", f"Simulator {version}")
    roots = (Path(r"C:\Program Files\PowerWorld"), Path(r"C:\Program Files (x86)\PowerWorld"))
    return tuple(root / name / "pwrworld.exe" for root in roots for name in names)


def find_powerworld_executable(version: int) -> Path:
    for candidate in powerworld_executable_candidates(version):
        if candidate.is_file():
            return candidate
    checked = "\n".join(f"  {path}" for path in powerworld_executable_candidates(version))
    raise PowerWorldError(
        f"PowerWorld Simulator {version} was not found. Checked:\n{checked}\n"
        "Confirm the selected Simulator version and installation folder."
    )


def launch_powerworld(
    case_path: str | Path,
    version: int,
    *,
    launcher: Callable[[Sequence[str]], Any] | None = None,
) -> Any:
    """Open a case in the licensed desktop application."""
    case = Path(case_path)
    if not case.is_file():
        raise PowerWorldError(f"The case file does not exist: {case}")
    executable = find_powerworld_executable(version)
    start = launcher or subprocess.Popen
    try:
        return start([str(executable), str(case)])
    except OSError as exc:
        raise PowerWorldError(f"PowerWorld could not open {case}: {exc}") from exc


class PowerWorldClient:
    """Small checked wrapper around the SimulatorAuto COM object."""

    def __init__(self, automation: Any | None = None) -> None:
        self._automation = automation

    def connect(self) -> None:
        if self._automation is not None:
            return
        try:
            import win32com.client
        except ImportError as exc:
            raise PowerWorldError(
                "pywin32 is required for PowerWorld automation. Install the project requirements first."
            ) from exc
        try:
            self._automation = win32com.client.DispatchEx("pwrworld.SimulatorAuto")
        except Exception as exc:
            raise PowerWorldError(
                "Could not start PowerWorld SimAuto. Confirm that Simulator is installed, licensed, "
                "and registered for automation."
            ) from exc

    def disconnect(self) -> None:
        """Release this client's COM reference after its case has been closed."""
        automation = self._automation
        self._automation = None
        if automation is None:
            return
        del automation
        gc.collect()
        try:
            import pythoncom

            pythoncom.CoFreeUnusedLibraries()
        except (ImportError, AttributeError):
            pass

    def open_case(self, path: str | Path) -> Any:
        case = Path(path)
        if not case.is_file():
            raise PowerWorldError(f"The case file does not exist: {case}")
        return self._call("OpenCase", str(case), context=f"PowerWorld could not open {case}")

    def close_case(self) -> Any:
        if self._automation is None:
            return ("",)
        return self._call("CloseCase", context="PowerWorld could not close the current case")

    def change_parameters_multiple(
        self, object_type: str, fields: Sequence[str], rows: Sequence[Sequence[object]]
    ) -> Any:
        """Send row-oriented updates in one COM call (not one call per contingency)."""
        if not rows:
            return ("",)
        if any(len(row) != len(fields) for row in rows):
            raise ValueError("Every update row must match the field count.")
        return self._call(
            "ChangeParametersMultipleElement", object_type, list(fields),
            [list(row) for row in rows], context=f"PowerWorld could not update {object_type}",
        )

    def save_case(self, path: str | Path, file_type: str = "PWB", overwrite: bool = False) -> Any:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        return self._call(
            "SaveCase",
            str(destination),
            file_type,
            overwrite,
            context=f"PowerWorld could not save {destination}",
        )

    def process_aux_file(self, path: str | Path) -> Any:
        aux_path = Path(path)
        if not aux_path.is_file():
            raise PowerWorldError(f"The AUX file does not exist: {aux_path}")
        return self._call(
            "ProcessAuxFile",
            str(aux_path),
            context=f"PowerWorld rejected {aux_path}",
        )

    def read_pti_contingencies(self, path: str | Path) -> Any:
        con_path = Path(path)
        if not con_path.is_file():
            raise PowerWorldError(f"The contingency file does not exist: {con_path}")
        escaped = str(con_path).replace('"', '""')
        return self.run_script_command(f'CTGReadFilePTI("{escaped}");')

    def run_script_command(self, command: str) -> Any:
        return self._call(
            "RunScriptCommand",
            command,
            context=f"PowerWorld script command failed: {command}",
        )

    def change_parameters_single(
        self,
        object_type: str,
        fields: Sequence[str],
        values: Sequence[object],
    ) -> Any:
        return self._call(
            "ChangeParametersSingleElement",
            object_type,
            list(fields),
            list(values),
            context=f"PowerWorld could not update {object_type}",
        )

    def set_all_distributed_computers_enabled(self, enabled: bool) -> Any:
        """Set Enabled on every existing DistributedComputer row.

        DistributedComputer is keyed by DSServerName.  A script such as
        ``SetData(DistributedComputer, [Enabled], ...)`` is rejected by
        PowerWorld because it does not identify the row being changed.
        Read the keys first, then send a keyed bulk update instead.
        """
        raw = self.get_parameters_multiple("DistributedComputer", ("DSServerName",), "")
        try:
            columns = raw[1]  # type: ignore[index]
            names = columns[0]
        except (IndexError, TypeError, KeyError):
            return ("",)
        rows = [(str(name), "YES" if enabled else "NO") for name in names if str(name).strip()]
        return self.change_parameters_multiple(
            "DistributedComputer", ("DSServerName", "Enabled"), rows
        )

    def get_parameters_multiple(
        self,
        object_type: str,
        fields: Sequence[str],
        filter_name: str = "",
    ) -> Any:
        return self._call(
            "GetParametersMultipleElement",
            object_type,
            list(fields),
            filter_name,
            context=f"PowerWorld could not retrieve {object_type}",
        )

    def write_aux_file(
        self,
        path: str | Path,
        filter_name: str,
        object_type: str,
        append: bool,
        fields: str | Sequence[str],
    ) -> Any:
        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        field_value: str | list[str] = fields if isinstance(fields, str) else list(fields)
        return self._call(
            "WriteAuxFile",
            str(destination),
            filter_name,
            object_type,
            append,
            field_value,
            context=f"PowerWorld could not write {destination}",
        )

    def _call(self, method_name: str, *args: Any, context: str) -> Any:
        self.connect()
        try:
            result = getattr(self._automation, method_name)(*args)
        except Exception as exc:
            raise PowerWorldError(f"{context}: {exc}") from exc
        self._raise_for_result(result, context)
        return result

    @staticmethod
    def _raise_for_result(result: Any, context: str) -> None:
        try:
            message = result[0]
        except (TypeError, IndexError, KeyError):
            return
        if message:
            raise PowerWorldError(f"{context}: {message}")
