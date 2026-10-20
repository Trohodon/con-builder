"""Contingency construction and first-pass PowerWorld analysis workflow."""

from __future__ import annotations

import csv
from copy import deepcopy
from dataclasses import dataclass, replace
from pathlib import Path
import time
from typing import Callable
from uuid import uuid4

from .execution import StudyExecution, is_worker_failure
from .recovery import RecoveryStore, StopRequested

from .aux_files import (
    CATEGORY_LABELS,
    COMMON_STRUCTURE,
    GENERIC_CONTINGENCY,
    GENERIC_CONTINGENCY_POST,
    JOIN_ACTIVE_CONTINGENCY,
    MANUAL,
    OPERATING_GUIDE,
    paths_for_case,
)
from .powerworld import PowerWorldClient, PowerWorldError
from .preparation import apply_input_file
from .settings import AppSettings
from .distributed import (
    PowerWorldComputerStatus,
    parse_powerworld_computer_statuses,
    write_distributed_aux,
)

LogCallback = Callable[[str], None]
BaseCaseDecision = Callable[[Path, int], bool]
BRANCH_RESULT_FIELDS = (
    "BusNum",
    "BusName",
    "BusNum:1",
    "BusName:1",
    "LineCircuit",
    "LineXfmr",
    "CTGViol",
    "LineMaxPercentContingency",
)
CSV_RESULT_FIELDS = (
    "CTGLabel",
    "LimViolID",
    "LimViolCat",
    "LimViolValue",
    "LimViolLimit",
    "LimViolPct",
    "LimViolCTGSpecifiedLimit",
    "CTGSolved",
)


@dataclass(slots=True)
class ContingencyOptions:
    n1: bool = True
    n11: bool = True
    long_term_p2: bool = True
    extreme_double_bus: bool = False
    extreme_double_branch: bool = False
    extreme_bus_branch: bool = False
    extreme_p1_p7: bool = False
    use_model_build: bool = False
    write_n1_models: bool = False
    evaluate_nonexistent: bool = True

    def validate(self) -> None:
        if not any(
            (
                self.n1,
                self.n11,
                self.long_term_p2,
                self.extreme_double_bus,
                self.extreme_double_branch,
                self.extreme_bus_branch,
                self.extreme_p1_p7,
            )
        ):
            raise ValueError("Select at least one contingency group.")
        if self.write_n1_models and not self.use_model_build:
            raise ValueError("N-1 model creation requires the Model Build method.")
        if self.use_model_build:
            raise ValueError(
                "The Model Build method and N-1 model writer are not available in this test "
                "release. Use the validated Join Active method."
            )


@dataclass(slots=True)
class AnalysisOptions:
    mode: str = "dc_ac"
    dc_monitor_percent: int = 80
    ac_monitor_percent: int = 90
    screen_tdf_cutoff: float = 2.0
    screen_volt_cutoff: float = 2.5
    do_not_rereport: bool = True
    rereport_tdf_cutoff: float = 0.5
    rereport_volt_cutoff: float = 0.5
    check_basecase: bool = True
    full_ac: bool = False
    contingencies_per_file: int = 80_000
    use_distributed: bool = True
    delete_skipped: bool = True
    create_pwb: bool = True
    create_aux: bool = False
    create_csv: bool = False

    def validate(self) -> None:
        if self.mode not in {"dc_ac", "ac_screen"}:
            raise ValueError("Unknown analysis mode.")
        if self.mode == "ac_screen":
            raise ValueError(
                "AC Screening is not available in this test release. The legacy implementation "
                "requires balancing-area screening AUX files and contains unfinished processing. "
                "Use DC with AC verification."
            )
        if not 0 <= self.dc_monitor_percent <= 32767:
            raise ValueError("DC CA Lim Mon % must be between 0 and 32767.")
        if not 0 <= self.ac_monitor_percent <= 32767:
            raise ValueError("AC CA Lim Mon % must be between 0 and 32767.")
        if self.mode == "ac_screen" and not 0 < self.screen_tdf_cutoff <= 100:
            raise ValueError("Screen TDF cutoff must be greater than 0 and no more than 100.")
        if self.mode == "ac_screen" and not 0 < self.screen_volt_cutoff <= 20:
            raise ValueError("Screen Volt cutoff must be greater than 0 and no more than 20.")
        if self.do_not_rereport and self.rereport_tdf_cutoff < 0:
            raise ValueError("TDF % cutoff cannot be negative.")
        if self.do_not_rereport and self.rereport_volt_cutoff < 0:
            raise ValueError("Volt % cutoff cannot be negative.")
        if self.full_ac and self.contingencies_per_file < 1:
            raise ValueError("Contingencies/file must be a positive integer.")
        if not any((self.create_pwb, self.create_aux, self.create_csv)):
            raise ValueError("Select at least one output type.")


@dataclass(slots=True)
class AnalysisResult:
    case_path: Path
    outputs: tuple[Path, ...]
    warnings: tuple[str, ...] = ()


class BaseCaseViolationError(PowerWorldError):
    """Raised when the user declines to continue after the base-case check."""


class ContingencyWorkflow:
    def __init__(
        self,
        client: PowerWorldClient,
        settings: AppSettings,
        *,
        resources_dir: Path | None = None,
        log: LogCallback | None = None,
        confirm_basecase_violations: BaseCaseDecision | None = None,
        distributed_retry_delays: tuple[float, ...] = (5.0, 15.0),
        distributed_pool_release_delay: float = 30.0,
        wait: Callable[[float], None] = time.sleep,
        client_factory: Callable[[], PowerWorldClient] = PowerWorldClient,
        progress: Callable[[dict], None] | None = None,
        stop_requested: Callable[[], bool] | None = None,
    ) -> None:
        self.client = client
        self.settings = deepcopy(settings)
        self.resources_dir = resources_dir or Path(__file__).resolve().parent.parent / "Resources"
        self.log = log or (lambda _message: None)
        self.confirm_basecase_violations = confirm_basecase_violations
        self.distributed_retry_delays = distributed_retry_delays
        self.distributed_pool_release_delay = distributed_pool_release_delay
        self.wait = wait
        self.client_factory = client_factory
        self.progress = progress or (lambda _event: None)
        self.stop_requested = stop_requested or (lambda: False)
        self._active_distributed_config = None
        self._runtime_aux: Path | None = None

    def prepare_combined_case(
        self,
        case_path: str | Path,
        output_path: str | Path,
        contingencies: ContingencyOptions,
    ) -> Path:
        contingencies.validate()
        case = Path(case_path)
        destination = Path(output_path)
        self.validate_inputs(case, contingencies)
        self._open_and_condition(case)
        try:
            self.build_contingencies(case, contingencies)
            self._log_contingency_count()
            self.log(f"Saving prepared contingency case: {destination}")
            self.client.save_case(destination, "PWB", True)
            return destination
        finally:
            self._close_safely()

    def write_pti_contingencies(
        self,
        case_path: str | Path,
        output_path: str | Path,
        contingencies: ContingencyOptions,
        *,
        truncate_labels: bool = False,
    ) -> Path:
        contingencies.validate()
        case = Path(case_path)
        destination = Path(output_path)
        self.validate_inputs(case, contingencies)
        self._open_and_condition(case)
        try:
            self.build_contingencies(case, contingencies)
            self._log_contingency_count()
            escaped = str(destination).replace('"', '""')
            truncate = "YES" if truncate_labels else "NO"
            self.client.run_script_command(f'CTGWriteFilePTI("{escaped}","Name12","{truncate}");')
            self.log(f"PTI contingency file written: {destination}")
            return destination
        finally:
            self._close_safely()

    def run_analysis(
        self,
        case_path: str | Path,
        output_dir: str | Path,
        contingencies: ContingencyOptions,
        analysis: AnalysisOptions,
    ) -> AnalysisResult:
        contingencies.validate()
        analysis.validate()
        self.settings.use_distributed = analysis.use_distributed
        case = Path(case_path)
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)
        self.validate_inputs(case, contingencies, include_basecase=analysis.check_basecase)
        self._log_aux_plan(case)
        if analysis.check_basecase and analysis.mode == "dc_ac":
            try:
                self._check_basecase(case, analysis)
            except Exception:
                self.client.disconnect()
                raise
        outputs: list[Path] = []
        warnings: list[str] = []
        groups = self._selected_groups(contingencies)
        for index, (label, group) in enumerate(groups, start=1):
            if self.stop_requested():
                raise StopRequested("Stopped before starting the next contingency group.")
            self.progress({"stage": f"Preparing group {index}/{len(groups)}: {label}"})
            if index > 1:
                self.client = self.client_factory()
            self.log(f"Starting contingency group {index} of {len(groups)}: {label}")
            try:
                self._open_and_condition(case)
                self.build_contingencies(case, group)
                if analysis.delete_skipped and not label.startswith("EE_"):
                    # The legacy workflow removes contingencies already marked SKIP
                    # before normal analysis. Its Extreme Events path leaves them in
                    # place through DC screening and removes them after AC instead.
                    self.client.run_script_command('Delete(Contingency, "SKIPPED");')
                self._log_contingency_count()
                force_ac = label in {"P1,2,4,7", "LongTerm"}
                if label == "P1,2,4,7":
                    self._configure_n1_controls()
                self._configure_analysis(analysis, force_ac=force_ac)
                self.log(f"Running contingency analysis for {label}.")
                if analysis.full_ac or (analysis.mode == "dc_ac" and force_ac):
                    run_type = "ACCA"
                elif analysis.mode == "dc_ac":
                    run_type = "DCwACver"
                else:
                    run_type = "scrCA"
                base = output_root / f"{case.stem}_{run_type}_{label}"
                manifest = output_root / f"{base.name}_{uuid4().hex[:8]}.resume.json"
                self._runtime_aux = manifest.with_suffix(".workers.aux")
                execution = StudyExecution(self, analysis, manifest)
                execution.start(force_ac=force_ac, output_base=base,
                                delete_skipped=analysis.delete_skipped and label != "P1,2,4,7")
                outputs.extend(execution.finish())
                warnings.extend(execution.warnings)
            finally:
                self._close_safely()
                self.client.disconnect()
        return AnalysisResult(case, tuple(outputs), tuple(warnings))

    def resume_analysis(self, manifest: str | Path) -> AnalysisResult:
        store = RecoveryStore(Path(manifest))
        data = store.load()
        if data["phase"] == "complete":
            raise PowerWorldError("This recovery record is already complete. Start a new study to rerun it.")
        options = AnalysisOptions(**data["options"])
        options.validate()
        self.settings.use_distributed = options.use_distributed
        self._runtime_aux = store.path.with_suffix(".workers.aux")
        execution = StudyExecution(self, options, store.path)
        execution.store = store
        self.log(f"Resuming {store.path}; saved stage: {data['phase']}. Current computer allocations will be used.")
        try:
            self.client.open_case(store.checkpoint)
            store.validate_open_case(self.client)
            execution.run()
            outputs = execution.finish()
            return AnalysisResult(store.checkpoint, tuple(outputs), tuple(execution.warnings))
        finally:
            self._close_safely()
            self.client.disconnect()

    def _log_aux_plan(self, case: Path) -> None:
        self.log(f"AUX/CON selections for {case.name}:")
        for category, label in CATEGORY_LABELS.items():
            selected = paths_for_case(self.settings.aux_files, category, str(case))
            if selected:
                self.log(f"  {label}: " + " | ".join(selected))
            else:
                self.log(f"  {label}: none")

    def _check_basecase(self, case: Path, options: AnalysisOptions) -> int:
        """Run the legacy BASECASE contingency once before any selected groups."""
        self.log(f"Checking {case.name} for base-case violations before analysis.")
        self._open_and_condition(case)
        try:
            self._apply_file(self.resources_dir / "Basecase.aux")
            line_percent = options.ac_monitor_percent
            if not options.full_ac:
                line_percent = min(options.dc_monitor_percent, options.ac_monitor_percent)
            self.client.change_parameters_single(
                "LimitSet",
                ("LSNum", "LSName", "LSLinePercent"),
                ("1", "Default", str(line_percent)),
            )
            self.client.run_script_command('CTGSolve("BASECASE");')
            result = self.client.get_parameters_multiple(
                "Contingency", ("CTGLabel", "CTGSkip", "CTGViol"), ""
            )
            try:
                labels, _skipped, violations = result[1]
            except (IndexError, TypeError, ValueError) as exc:
                raise PowerWorldError(
                    "PowerWorld returned an unexpected BASECASE result table."
                ) from exc

            violation_count: int | None = None
            for label, value in zip(labels, violations):
                if str(label).strip().casefold() == "basecase":
                    if value is None or str(value).strip() == "":
                        violation_count = 0
                    else:
                        violation_count = int(float(str(value)))
                    break
            if violation_count is None:
                raise PowerWorldError(
                    "PowerWorld did not return the BASECASE contingency after loading Basecase.aux."
                )

            self.log(f"Base case violations found: {violation_count}")
            if violation_count > 0:
                should_continue = (
                    self.confirm_basecase_violations(case, violation_count)
                    if self.confirm_basecase_violations is not None
                    else False
                )
                if not should_continue:
                    raise BaseCaseViolationError(
                        f"Analysis stopped because {case.name} has {violation_count} base-case "
                        "violation(s). Correct them, or turn off the base-case check to continue."
                    )
                self.log("Continuing after the base-case violation warning.")
            return violation_count
        finally:
            self._close_safely()

    def validate_inputs(
        self,
        case: Path,
        options: ContingencyOptions,
        *,
        include_basecase: bool = False,
    ) -> None:
        required: list[Path] = [case, self.resources_dir / "Conts_Filters.aux"]
        if include_basecase:
            required.append(self.resources_dir / "Basecase.aux")
        if options.n1 or options.n11 or options.extreme_p1_p7:
            required.append(self.resources_dir / "P1_All_Except_CB-CB.aux")
            required.extend(
                Path(path)
                for path in paths_for_case(self.settings.aux_files, GENERIC_CONTINGENCY, str(case))
            )
        if options.n1 or options.n11:
            required.append(self.resources_dir / "P2_All_Except_BF.aux")
            for category in (
                JOIN_ACTIVE_CONTINGENCY,
                GENERIC_CONTINGENCY_POST,
                COMMON_STRUCTURE,
            ):
                required.extend(
                    Path(path)
                    for path in paths_for_case(self.settings.aux_files, category, str(case))
                )
        if options.n11:
            if options.use_model_build:
                required.append(self.resources_dir / "N-2_Branches&TFMRs.aux")
            required.extend(
                Path(path)
                for path in paths_for_case(self.settings.aux_files, OPERATING_GUIDE, str(case))
            )
        if options.long_term_p2:
            required.append(self.resources_dir / "UG_Lines.aux")
            required.append(self.resources_dir / "P2_All_Except_BF.aux")
            required.extend(
                Path(path)
                for path in paths_for_case(
                    self.settings.aux_files,
                    JOIN_ACTIVE_CONTINGENCY,
                    str(case),
                )
            )
        if options.extreme_bus_branch or options.extreme_double_branch:
            required.append(self.resources_dir / "N-1_All.aux")
            required.extend(
                Path(path)
                for path in paths_for_case(self.settings.aux_files, GENERIC_CONTINGENCY, str(case))
            )
        if options.extreme_double_branch:
            required.append(self.resources_dir / "N-2_Branches&TFMRs.aux")
        if options.extreme_double_bus:
            required.append(self.resources_dir / "EE_DBL_Bus.aux")
        if options.extreme_bus_branch:
            required.append(self.resources_dir / "P2_Buses.aux")
        required.extend(
            Path(path)
            for path in paths_for_case(self.settings.aux_files, MANUAL, str(case))
        )
        if options.extreme_p1_p7:
            required.extend(
                Path(path)
                for path in paths_for_case(self.settings.aux_files, COMMON_STRUCTURE, str(case))
            )

        missing = list(dict.fromkeys(path for path in required if not path.is_file()))
        if missing:
            listing = "\n".join(f"  {path}" for path in missing)
            raise PowerWorldError(f"Required input files are missing or unavailable:\n{listing}")

    @staticmethod
    def _selected_groups(options: ContingencyOptions) -> list[tuple[str, ContingencyOptions]]:
        empty = replace(
            options,
            n1=False,
            n11=False,
            long_term_p2=False,
            extreme_double_bus=False,
            extreme_double_branch=False,
            extreme_bus_branch=False,
            extreme_p1_p7=False,
        )
        groups: list[tuple[str, ContingencyOptions]] = []
        if options.n1:
            groups.append(("P1,2,4,7", replace(empty, n1=True)))
        if options.n11:
            groups.append(("P1-7", replace(empty, n11=True)))
        if options.long_term_p2:
            groups.append(("LongTerm", replace(empty, long_term_p2=True)))
        if options.extreme_bus_branch:
            groups.append(("EE_BusBranch", replace(empty, extreme_bus_branch=True)))
        if options.extreme_double_branch:
            groups.append(("EE_DBL_Branch", replace(empty, extreme_double_branch=True)))
        if options.extreme_double_bus:
            groups.append(("EE_DBL_Bus", replace(empty, extreme_double_bus=True)))
        if options.extreme_p1_p7:
            groups.append(("EE_P1+P7", replace(empty, extreme_p1_p7=True)))
        return groups

    def _write_outputs(self, base: Path, analysis: AnalysisOptions) -> list[Path]:
        outputs: list[Path] = []
        def publish(temporary: Path, destination: Path) -> None:
            if not temporary.is_file() or not temporary.stat().st_size:
                raise PowerWorldError(f"No usable output was written: {temporary}")
            temporary.replace(destination)

        token = uuid4().hex[:8]
        if analysis.create_pwb:
            pwb = base.with_name(base.name + ".PWB")
            pending = base.with_name(f"{base.name}.{token}.pending.PWB")
            self.client.save_case(pending, "PWB", True)
            publish(pending, pwb)
            outputs.append(pwb)
            self.log(f"PWB output written: {pwb}")
        if analysis.create_aux:
            aux = base.with_name(base.name + ".AUX")
            pending = base.with_name(f"{base.name}.{token}.pending.AUX")
            self.client.write_aux_file(pending, "", "contingency", False, "all")
            self.client.write_aux_file(pending, "", "branch", True, BRANCH_RESULT_FIELDS)
            publish(pending, aux)
            outputs.append(aux)
            self.log(f"AUX output written: {aux}")
        if analysis.create_csv:
            csv_path = base.with_name(base.name + ".CSV")
            pending = base.with_name(f"{base.name}.{token}.pending.CSV")
            self._write_csv(pending)
            publish(pending, csv_path)
            outputs.append(csv_path)
            self.log(f"CSV output written: {csv_path}")
        return outputs

    def _open_and_condition(self, case: Path) -> None:
        if not case.is_file():
            raise PowerWorldError(f"The case file does not exist: {case}")
        self.log(f"Opening case: {case}")
        self.client.open_case(case)
        try:
            if self.settings.use_distributed:
                distributed_config = self.settings.distributed_config()
                mode = (
                    "multiple computers"
                    if self.settings.use_multiple_computers
                    else "local computer only"
                )
                self.log(
                    f"Distributed mode: {mode}; "
                    f"{len(distributed_config.computers)} computer(s) configured for this run."
                )
                self._apply_distributed_config(distributed_config)
            for path in paths_for_case(self.settings.aux_files, MANUAL, str(case)):
                self._apply_file(path)
            self.client.run_script_command("SolvePowerFlow;")
        except Exception:
            self._close_safely()
            raise

    def settings_store_aux_path(self) -> Path:
        from .settings import SettingsStore

        return self._runtime_aux or SettingsStore().distributed_aux_path

    def _apply_distributed_config(self, config) -> None:
        # Loading an AUX merges rows. Disable old rows first so a saved case cannot
        # silently bring back machines excluded by this run's settings.
        self.client.set_all_distributed_computers_enabled(False)
        path = self.settings_store_aux_path()
        write_distributed_aux(path, config)
        self._apply_file(path)

    def _temporary_contingency_aux_path(self) -> Path:
        from .settings import SettingsStore

        store = SettingsStore()
        store.base_dir.mkdir(parents=True, exist_ok=True)
        return store.base_dir / "P_All.aux"

    def build_contingencies(self, case: Path, options: ContingencyOptions) -> None:
        self.log("Clearing contingencies already stored in the case.")
        self.client.run_script_command('Delete(Contingency, "");')

        if options.n1 or options.n11 or options.extreme_p1_p7:
            self._apply_file(self.resources_dir / "P1_All_Except_CB-CB.aux")
            self._apply_category(case, GENERIC_CONTINGENCY)

        if options.evaluate_nonexistent and (options.n1 or options.n11):
            self._mark_nonexistent_contingencies_skipped()

        if options.n11:
            if options.use_model_build:
                self.log("Using the model-build N-1-1 path.")
                self._apply_file(self.resources_dir / "N-2_Branches&TFMRs.aux")
            else:
                self.log("Joining active N-1 contingencies to build N-1-1 contingencies.")
                self.client.run_script_command("CTGJoinActiveCTGs(NO,NO,YES);")

        # Both legacy P1,2,4,7 and P1-7 runs add the P2/non-combinable
        # stages after creating the base P1 contingencies.
        if options.n1 or options.n11:
            self._apply_file(self.resources_dir / "P2_All_Except_BF.aux")
            self._apply_category(case, JOIN_ACTIVE_CONTINGENCY)
            self._apply_category(case, GENERIC_CONTINGENCY_POST)
            self._apply_category(case, COMMON_STRUCTURE)

        if options.long_term_p2:
            ug_file = self.resources_dir / "UG_Lines.aux"
            saved_contingencies: Path | None = None
            if options.n1 or options.n11:
                saved_contingencies = self._temporary_contingency_aux_path()
                if saved_contingencies.exists():
                    saved_contingencies.unlink()
                escaped_saved = str(saved_contingencies).replace('"', '""')
                self.log("Temporarily saving the existing N-1/N-1-1 contingency set.")
                self.client.run_script_command(
                    f'SaveData("{escaped_saved}", AUX, CONTINGENCY,'
                    '[CTGLabel],[CTGElement],,[],NO);'
                )

            self.client.run_script_command('Delete(Contingency, "");')
            self._apply_file(self.resources_dir / "P2_All_Except_BF.aux")
            self._apply_category(case, JOIN_ACTIVE_CONTINGENCY)
            escaped_ug = str(ug_file).replace('"', '""')
            self.client.run_script_command(
                f'CTGJoinActiveCTGs(NO,YES,NO,"{escaped_ug}");'
            )

            if saved_contingencies is not None:
                self.log("Reloading the saved N-1/N-1-1 contingency set.")
                self._apply_file(saved_contingencies)

        if options.extreme_bus_branch:
            self._apply_file(self.resources_dir / "N-1_All.aux")
            self._apply_category(case, GENERIC_CONTINGENCY)
            self._apply_file(self.resources_dir / "Conts_Filters.aux")
            if options.evaluate_nonexistent:
                self._mark_nonexistent_contingencies_skipped()
            p2_buses = self.resources_dir / "P2_Buses.aux"
            escaped = str(p2_buses).replace('"', '""')
            self.client.run_script_command(
                f'CTGJoinActiveCTGs(NO,YES,NO,"{escaped}");'
            )
        if options.extreme_double_branch:
            self._apply_category(case, GENERIC_CONTINGENCY)
            self._apply_file(self.resources_dir / "Conts_Filters.aux")
            if options.evaluate_nonexistent:
                self._mark_nonexistent_contingencies_skipped()
            n1_all = self.resources_dir / "N-1_All.aux"
            escaped = str(n1_all).replace('"', '""')
            self.client.run_script_command(
                f'CTGJoinActiveCTGs(NO,YES,NO,"{escaped}");'
            )
            self._apply_file(self.resources_dir / "N-2_Branches&TFMRs.aux")
        if options.extreme_double_bus:
            self._apply_file(self.resources_dir / "Conts_Filters.aux")
            self._apply_file(self.resources_dir / "EE_DBL_Bus.aux")
        if options.extreme_p1_p7:
            self._apply_file(self.resources_dir / "Conts_Filters.aux")
            if options.evaluate_nonexistent:
                self._mark_nonexistent_contingencies_skipped()
            self._apply_category(case, COMMON_STRUCTURE, join_active=True)

        if options.n11:
            self._apply_category(case, OPERATING_GUIDE)
        if not any(
            (
                options.extreme_bus_branch,
                options.extreme_double_branch,
                options.extreme_double_bus,
                options.extreme_p1_p7,
            )
        ):
            self._apply_file(self.resources_dir / "Conts_Filters.aux")

    def _mark_nonexistent_contingencies_skipped(self) -> int:
        """Match the active part of the legacy removeDupConts routine.

        The duplicate-removal portion was commented out in VB. The working part
        marks a contingency skipped when PowerWorld cannot resolve its element.
        """
        self.log("Evaluating contingencies for elements that do not exist in the case.")
        result = self.client.get_parameters_multiple(
            "ContingencyElement", ("CTGLabel", "WhoAmI"), ""
        )
        try:
            columns = result[1]
            labels, identities = columns[0], columns[1]
        except (IndexError, TypeError, ValueError) as exc:
            raise PowerWorldError(
                "PowerWorld returned an unexpected contingency-element table."
            ) from exc

        skipped_labels: list[str] = []
        seen: set[str] = set()
        for label, identity in zip(labels, identities):
            clean_label = str(label).strip()
            if (
                clean_label
                and clean_label.casefold() != "basecase"
                and not str(identity or "").strip()
                and clean_label not in seen
            ):
                self.client.change_parameters_single(
                    "Contingency",
                    ("CTGLabel", "CTGSkip"),
                    (clean_label, "YES"),
                )
                seen.add(clean_label)
                skipped_labels.append(clean_label)

        self.log(
            f"Marked {len(skipped_labels)} non-existent contingenc"
            f"{'y' if len(skipped_labels) == 1 else 'ies'} as skipped."
        )
        return len(skipped_labels)

    def _configure_analysis(self, options: AnalysisOptions, *, force_ac: bool = False) -> None:
        calculation_method = (
            "0" if options.full_ac or options.mode == "ac_screen" or force_ac else "1"
        )
        self.client.change_parameters_single(
            "Ctg_Options",
            ("CTG_CalculationMethod",),
            (calculation_method,),
        )
        monitor_percent = options.ac_monitor_percent if calculation_method == "0" else options.dc_monitor_percent
        self.client.change_parameters_single(
            "LimitSet",
            ("LSNum", "LSName", "LSLinePercent"),
            ("1", "Default", str(monitor_percent)),
        )
        self.client.change_parameters_single(
            "Ctg_Options",
            ("CTG_WhatToDoWithBC", "CTG_BCFlows", "CTG_BCLowVolt", "CTG_BCHighVolt"),
            (
                "2" if options.do_not_rereport else "1",
                str(options.rereport_tdf_cutoff),
                str(options.rereport_volt_cutoff / 100),
                str(options.rereport_volt_cutoff / 100),
            ),
        )

    def _configure_n1_controls(self) -> None:
        """Match the three control options set by the legacy N-1 workflow."""
        self.log("Disabling shunt, tap, and LTC tap-balance checks for N-1 analysis.")
        for option in ("ChkShunts", "ChkTaps", "LTCTapBalance"):
            self.client.run_script_command(
                f'SetData(CTG_Options_Value, [Option, Value], ["{option}", "NO"]);'
            )

    def _verify_distributed_workers(self, stage: str) -> tuple[PowerWorldComputerStatus, ...]:
        delays = (0.0,) + self.distributed_retry_delays
        last_error: Exception | None = None
        for attempt, delay in enumerate(delays, start=1):
            if delay:
                self.log(
                    f"No distributed workers were available for {stage}. "
                    f"Waiting {delay:g} seconds before verification attempt {attempt} of {len(delays)}."
                )
                self.wait(delay)
                self._apply_distributed_config(self._active_distributed_config or self.settings.distributed_config())

            self.log(
                f"Verifying distributed computers for {stage} "
                f"(attempt {attempt} of {len(delays)})."
            )
            try:
                self.client.run_script_command("VerifyDistributedComputersAvailable;")
                raw = self.client.get_parameters_multiple(
                    "DistributedComputer",
                    (
                        "DSServerName",
                        "Enabled:1",
                        "NumberOfConnections",
                        "NumberOfConnections:1",
                        "DistNumErrors",
                    ),
                    "",
                )
                configured = self._active_distributed_config or self.settings.distributed_config()
                names = {computer.name.strip().casefold() for computer in configured.computers}
                statuses = tuple(status for status in parse_powerworld_computer_statuses(raw)
                                 if status.name.casefold() in names)
            except (PowerWorldError, ValueError) as exc:
                last_error = exc
                self.log(f"Distributed verification attempt {attempt} failed: {exc}")
                if isinstance(exc, ValueError) or not is_worker_failure(exc):
                    raise PowerWorldError(f"Distributed verification failed: {exc}") from exc
                continue

            ready = tuple(status for status in statuses if status.available and status.requested_workers > 0)
            requested_total = sum(status.requested_workers for status in statuses)
            ready_workers = sum(status.requested_workers for status in ready)
            detected_total = sum(status.detected_cores for status in ready)
            for status in statuses:
                state = "READY" if status.available else "UNAVAILABLE"
                self.log(
                    f"  {status.name}: {state}; requested workers={status.requested_workers}; "
                    f"detected cores={status.detected_cores}; errors={status.errors}"
                )
            self.log(
                f"Distributed capacity for {stage}: {len(ready)} of {len(statuses)} computers, "
                f"{ready_workers} of {requested_total} requested workers, "
                f"{detected_total} detected cores available."
            )
            if ready:
                if len(ready) != len(statuses):
                    self.log("WARNING: Continuing with the distributed computers that are available.")
                self.progress({"stage": f"Workers ready for {stage}", "workers": ready_workers,
                               "computers": len(ready)})
                return statuses
            last_error = PowerWorldError(
                f"PowerWorld reported 0 of {len(statuses)} distributed computers available for {stage}."
            )

        detail = f": {last_error}" if last_error is not None else ""
        raise PowerWorldError(
            f"No distributed computers are available for {stage} after {len(delays)} attempts{detail}"
        )

    def _run_solve_all(
        self,
        *,
        distributed: bool,
        stage: str,
        preserve_results: bool = False,
    ) -> None:
        if distributed:
            statuses = self._verify_distributed_workers(stage)
            # Verification can change Enabled. Enforce the configured subset again
            # without resetting availability or error counters.
            self.client.set_all_distributed_computers_enabled(False)
            ready = [status for status in statuses if status.available and status.requested_workers > 0]
            self.client.change_parameters_multiple(
                "DistributedComputer", ("DSServerName", "Enabled"),
                [(status.name, "YES") for status in ready],
            )
            self.progress({"stage": stage, "workers": sum(s.requested_workers for s in ready),
                           "computers": len(ready)})
        else:
            self.progress({"stage": stage + " (local)"})
        if self.stop_requested():
            raise StopRequested(f"Stopped before starting {stage}.")
        suffix = ",NO" if preserve_results else ""
        command = f"CTGSolveAll({'YES' if distributed else 'NO'}{suffix});"
        if distributed:
            self.log(f"Distributed workers accepted. PowerWorld is now running {stage}.")
        else:
            self.log(f"PowerWorld is now running {stage}.")
        self.client.run_script_command(command)

    def _restart_from_checkpoint(self, checkpoint: Path, stage: str) -> None:
        """Release a failed/finished pool and reopen a known-good case state."""
        self.log(f"Closing the current SimAuto session before {stage}.")
        self._close_safely()
        self.client.disconnect()

        if self.distributed_pool_release_delay > 0:
            self.log(
                f"Waiting {self.distributed_pool_release_delay:g} seconds for distributed "
                "workers to be released."
            )
            self.wait(self.distributed_pool_release_delay)

        self.log(f"Starting a fresh SimAuto session for {stage}.")
        self.client = self.client_factory()
        self.client.open_case(checkpoint)
        self.log(f"Recovery checkpoint opened for {stage}: {checkpoint}")

    def _apply_category(self, case: Path, category: str, *, join_active: bool = False) -> None:
        for path in paths_for_case(self.settings.aux_files, category, str(case)):
            if join_active:
                escaped = str(path).replace('"', '""')
                self.client.run_script_command(f'CTGJoinActiveCTGs(NO,YES,NO,"{escaped}");')
            else:
                self._apply_file(path)

    def _apply_file(self, path: str | Path) -> None:
        self.log(f"Applying file: {path}")
        apply_input_file(self.client, path)

    def _write_csv(self, path: Path) -> None:
        result = self.client.get_parameters_multiple("Contingency", CSV_RESULT_FIELDS, "")
        try:
            columns = result[1]
            rows = zip(*columns)
        except (IndexError, TypeError) as exc:
            raise PowerWorldError("PowerWorld returned an unexpected contingency result table.") from exc
        with path.open("w", newline="", encoding="utf-8-sig") as stream:
            writer = csv.writer(stream)
            writer.writerow(CSV_RESULT_FIELDS)
            writer.writerows(rows)

    def _log_contingency_count(self) -> int:
        result = self.client.get_parameters_multiple("Contingency", ("CTGLabel",), "")
        try:
            count = len(result[1][0])
        except (IndexError, TypeError) as exc:
            raise PowerWorldError("PowerWorld returned an unexpected contingency list.") from exc
        self.log(f"Contingencies currently loaded: {count}")
        return count

    def _close_safely(self) -> None:
        try:
            self.client.close_case()
        except PowerWorldError as exc:
            self.log(f"Warning: {exc}")
