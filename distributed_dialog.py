from __future__ import annotations

import queue
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
import tkinter as tk
from tkinter import messagebox, ttk

from core.distributed import (
    ComputerVerification,
    DistributedComputer,
    DistributedConfig,
    is_local_computer_name,
    parse_powerworld_verification,
    verify_computer,
    write_distributed_aux,
)
from core.powerworld import PowerWorldClient
from core.settings import AppSettings, SettingsStore


class DistributedDialog(tk.Toplevel):
    def __init__(self, parent: tk.Misc, settings: AppSettings, store: SettingsStore) -> None:
        super().__init__(parent)
        self.settings = settings
        self.store = store
        self.title("Distributed Computing")
        self.geometry("940x620")
        self.minsize(820, 560)
        self.transient(parent)
        self.grab_set()

        self.ctg_var = tk.StringVar(value=str(settings.contingencies_per_process))
        self.multi_var = tk.BooleanVar(value=settings.use_multiple_computers)
        self.fallback_var = tk.BooleanVar(value=settings.allow_local_fallback)
        self.resize_var = tk.BooleanVar(value=settings.size_worker_pool)
        self.name_var = tk.StringVar()
        self.cores_var = tk.StringVar(value="1")
        self._checking = False
        self._verification_queue: queue.Queue[
            tuple[list[str], list[ComputerVerification], str | None]
        ] = queue.Queue()

        body = ttk.Frame(self, padding=14)
        body.pack(fill="both", expand=True)

        heading = ttk.Frame(body)
        heading.pack(fill="x")
        ttk.Checkbutton(
            heading,
            text="Use multiple computers",
            variable=self.multi_var,
            command=self._mode_changed,
        ).pack(side="left", padx=(0, 24))
        ttk.Label(heading, text="Contingencies per process").pack(side="left")
        ttk.Entry(heading, textvariable=self.ctg_var, width=10).pack(side="left", padx=(8, 24))
        ttk.Label(
            heading,
            text="Cores/workers is PowerWorld's Number of Connections for that computer.",
        ).pack(side="left")

        policies = ttk.Frame(body)
        policies.pack(fill="x", pady=(10, 0))
        ttk.Checkbutton(policies, text="Limit workers to the work available in each stage",
                        variable=self.resize_var).pack(anchor="w")
        ttk.Checkbutton(policies, text="Allow local fallback after distributed retries fail (may be much slower)",
                        variable=self.fallback_var).pack(anchor="w", pady=(4, 0))
        ttk.Label(policies, text="Otherwise the run stops with a checkpoint you can resume.").pack(anchor="w")

        table = ttk.Frame(body)
        table.pack(fill="both", expand=True, pady=(12, 8))
        self.tree = ttk.Treeview(
            table,
            columns=("name", "cores", "status"),
            show="headings",
            selectmode="browse",
            height=10,
        )
        self.tree.heading("name", text="Computer name")
        self.tree.heading("cores", text="Cores / workers")
        self.tree.heading("status", text="Verification")
        self.tree.column("name", width=280, anchor="w")
        self.tree.column("cores", width=115, anchor="center")
        self.tree.column("status", width=270, anchor="w")
        scroll = ttk.Scrollbar(table, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        self.tree.bind("<<TreeviewSelect>>", self._selection_changed)

        editor = ttk.LabelFrame(body, text="Selected computer", padding=8)
        editor.pack(fill="x")
        ttk.Label(editor, text="Computer name").grid(row=0, column=0, sticky="w")
        self.name_entry = ttk.Entry(editor, textvariable=self.name_var, width=32)
        self.name_entry.grid(row=1, column=0, sticky="ew", padx=(0, 10))
        ttk.Label(editor, text="Cores / workers").grid(row=0, column=1, sticky="w")
        ttk.Spinbox(editor, from_=1, to=999, textvariable=self.cores_var, width=10).grid(
            row=1, column=1, sticky="w", padx=(0, 10)
        )
        ttk.Button(editor, text="Apply Changes", command=self._apply_editor).grid(
            row=1, column=2, sticky="w"
        )
        editor.columnconfigure(0, weight=1)

        row_buttons = ttk.Frame(body)
        row_buttons.pack(fill="x", pady=(8, 0))
        ttk.Button(row_buttons, text="Add Blank Computer", command=self._add_blank).pack(side="left")
        ttk.Button(row_buttons, text="Remove Selected", command=self._remove).pack(side="left", padx=8)
        self.verify_button = ttk.Button(row_buttons, text="Verify List", command=self._verify_list)
        self.verify_button.pack(side="left", padx=(16, 0))
        ttk.Label(
            row_buttons,
            text="Blank and local-only remote rows are ignored during verification.",
        ).pack(side="left", padx=12)

        buttons = ttk.Frame(body)
        buttons.pack(fill="x", pady=(14, 0))
        ttk.Button(buttons, text="Cancel", command=self.destroy).pack(side="right")
        ttk.Button(buttons, text="Save Configuration", command=self._save).pack(
            side="right", padx=8
        )

        for item in settings.distributed_computers:
            cores = item.get("cores", item.get("connections", 1))
            name = str(item.get("name", ""))
            status = self._status_before_check(name)
            self.tree.insert("", "end", values=(name, cores, status))
        if not self.tree.get_children():
            self._add_blank()
        else:
            first = self.tree.get_children()[0]
            self.tree.selection_set(first)
            self.tree.focus(first)
            self._selection_changed()

    def _computer_is_active(self, name: str) -> bool:
        return self.multi_var.get() or is_local_computer_name(name)

    def _status_before_check(self, name: str) -> str:
        if not name.strip():
            return "Blank - ignored when running"
        if not self._computer_is_active(name):
            return "Saved - ignored in local-only mode"
        return "Not checked"

    def _mode_changed(self) -> None:
        for item in self.tree.get_children():
            values = list(self.tree.item(item, "values"))
            values[2] = self._status_before_check(str(values[0]))
            self.tree.item(item, values=values)

    def _selection_changed(self, _event=None) -> None:
        selected = self.tree.selection()
        if not selected:
            return
        values = self.tree.item(selected[0], "values")
        self.name_var.set(str(values[0]))
        self.cores_var.set(str(values[1]))

    def _add_blank(self) -> None:
        item = self.tree.insert("", "end", values=("", 1, "Blank - ignored when running"))
        self.tree.selection_set(item)
        self.tree.focus(item)
        self.tree.see(item)
        self._selection_changed()
        self.name_entry.focus_set()

    def _apply_editor(self, *, show_error: bool = True) -> bool:
        selected = self.tree.selection()
        if not selected:
            return True
        name = self.name_var.get().strip()
        try:
            cores = int(self.cores_var.get())
            if cores < 1:
                raise ValueError
            if name:
                DistributedComputer(name, cores).validate()
        except ValueError:
            if show_error:
                messagebox.showerror(
                    "Invalid computer",
                    "Cores/workers must be a positive whole number, and the computer name cannot contain quotes.",
                    parent=self,
                )
            return False
        status = self._status_before_check(name)
        self.tree.item(selected[0], values=(name, cores, status))
        return True

    def _remove(self) -> None:
        selected = self.tree.selection()
        if not selected:
            return
        item = selected[0]
        siblings = list(self.tree.get_children())
        index = siblings.index(item)
        self.tree.delete(item)
        remaining = self.tree.get_children()
        if remaining:
            next_item = remaining[min(index, len(remaining) - 1)]
            self.tree.selection_set(next_item)
            self.tree.focus(next_item)
            self._selection_changed()
        else:
            self.name_var.set("")
            self.cores_var.set("1")

    def _verify_list(self) -> None:
        if self._checking or not self._apply_editor():
            return
        items = list(self.tree.get_children())
        names = [str(self.tree.item(item, "values")[0]) for item in items]
        use_multiple = self.multi_var.get()
        active = [use_multiple or is_local_computer_name(name) for name in names]
        computers = tuple(
            DistributedComputer(str(values[0]).strip(), int(values[1]))
            for item, is_active in zip(items, active)
            if (values := self.tree.item(item, "values"))
            and str(values[0]).strip()
            and is_active
        )
        if not computers:
            messagebox.showerror(
                "Distributed computing",
                "Local-only mode requires a localhost computer row.",
                parent=self,
            )
            return
        try:
            ctg_per_process = int(self.ctg_var.get())
            config = DistributedConfig(ctg_per_process, computers)
            config.validate()
        except ValueError as exc:
            messagebox.showerror("Distributed computing", str(exc), parent=self)
            return
        self._checking = True
        self.verify_button.state(("disabled",))
        for item, name, is_active in zip(items, names, active):
            values = list(self.tree.item(item, "values"))
            if name.strip() and is_active:
                values[2] = "Checking..."
            elif name.strip():
                values[2] = "Saved - ignored in local-only mode"
            else:
                values[2] = "Blank - ignored when running"
            self.tree.item(item, values=values)

        def check() -> None:
            pythoncom = None
            client = None
            warning = None
            try:
                try:
                    import pythoncom as _pythoncom

                    pythoncom = _pythoncom
                    pythoncom.CoInitialize()
                except ImportError:
                    pass
                with TemporaryDirectory(prefix="nerc_tpl_dist_verify_") as folder:
                    aux_path = write_distributed_aux(Path(folder) / "verify.aux", config)
                    client = PowerWorldClient()
                    client.set_all_distributed_computers_enabled(False)
                    client.process_aux_file(aux_path)
                    client.run_script_command("VerifyDistributedComputersAvailable;")
                    raw = client.get_parameters_multiple(
                        "DistributedComputer",
                        ("DSServerName", "Enabled:1", "NumberOfConnections:1", "DistNumErrors"),
                        "",
                    )
                    statuses = parse_powerworld_verification(raw)
                results = [
                    ComputerVerification(None, "Blank - ignored when running")
                    if not name.strip()
                    else ComputerVerification(None, "Saved - ignored in local-only mode")
                    if not is_active
                    else statuses.get(
                        name.strip().casefold(),
                        ComputerVerification(False, "Not returned by PowerWorld"),
                    )
                    for name, is_active in zip(names, active)
                ]
            except Exception as exc:
                results = [
                    verify_computer(name)
                    if name.strip() and is_active
                    else ComputerVerification(
                        None,
                        "Saved - ignored in local-only mode"
                        if name.strip()
                        else "Blank - ignored when running",
                    )
                    for name, is_active in zip(names, active)
                ]
                warning = (
                    "PowerWorld could not complete its distributed-computer verification. "
                    "The displayed results are network checks only.\n\n"
                    f"{exc}"
                )
            finally:
                if client is not None:
                    client.disconnect()
                if pythoncom is not None:
                    pythoncom.CoUninitialize()
            self._verification_queue.put((items, results, warning))

        threading.Thread(target=check, daemon=True).start()
        self.after(100, self._poll_verification)

    def _poll_verification(self) -> None:
        try:
            items, results, warning = self._verification_queue.get_nowait()
        except queue.Empty:
            if self._checking:
                self.after(100, self._poll_verification)
            return
        self._verification_finished(items, results, warning)

    def _verification_finished(self, items, results, warning: str | None) -> None:
        for item, result in zip(items, results):
            if self.tree.exists(item):
                values = list(self.tree.item(item, "values"))
                values[2] = result.message
                self.tree.item(item, values=values)
        self._checking = False
        self.verify_button.state(("!disabled",))
        if warning:
            messagebox.showwarning("Distributed verification", warning, parent=self)

    def _save(self) -> None:
        if not self._apply_editor():
            return
        try:
            computers = tuple(
                DistributedComputer(str(values[0]).strip(), int(values[1]))
                for item in self.tree.get_children()
                if (values := self.tree.item(item, "values")) and str(values[0]).strip()
            )
            configured = DistributedConfig(int(self.ctg_var.get()), computers)
            configured.validate()
            active_computers = (
                configured.computers
                if self.multi_var.get()
                else tuple(
                    computer
                    for computer in configured.computers
                    if is_local_computer_name(computer.name)
                )
            )
            config = DistributedConfig(configured.contingencies_per_process, active_computers)
            config.validate()
            write_distributed_aux(self.store.distributed_aux_path, config)
        except (OSError, ValueError) as exc:
            messagebox.showerror("Distributed computing", str(exc), parent=self)
            return

        self.settings.contingencies_per_process = config.contingencies_per_process
        self.settings.use_multiple_computers = self.multi_var.get()
        self.settings.allow_local_fallback = self.fallback_var.get()
        self.settings.size_worker_pool = self.resize_var.get()
        self.settings.distributed_computers = [
            {"name": computer.name.strip(), "cores": computer.connections}
            for computer in configured.computers
        ]
        self.store.save(self.settings)
        messagebox.showinfo(
            "Distributed computing",
            f"Saved {len(configured.computers)} computer(s); "
            f"{len(config.computers)} active in the selected mode.\n\n"
            f"Wrote {self.store.distributed_aux_path}",
            parent=self,
        )
        self.destroy()
