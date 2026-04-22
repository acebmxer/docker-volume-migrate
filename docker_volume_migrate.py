#!/usr/bin/env python3
"""Migrate Docker bind mounts to named volumes or a target directory (e.g. NFS mount point)."""

from __future__ import annotations

import argparse
import copy
import logging
import os
import re
import shutil
import signal
import sys
from dataclasses import dataclass, field
from typing import Optional

import docker
import docker.errors
from docker.types import LogConfig, Mount
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

console = Console()
log = logging.getLogger("docker_volume_migrate")

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class BindMount:
    source: str
    destination: str
    read_write: bool
    mode: str
    propagation: str
    type: str = "bind"  # "bind" or "volume"


@dataclass
class ContainerInfo:
    id: str
    name: str
    state: str
    image: str
    mounts: list[BindMount]
    is_compose_managed: bool
    compose_project: Optional[str]
    compose_service: Optional[str]
    compose_config_files: Optional[str]
    compose_working_dir: Optional[str]


@dataclass
class MountPlan:
    mount: BindMount
    volume_name: str
    target_path: Optional[str] = None  # directory mode: full destination path on host
    skip: bool = False

    @property
    def is_directory_mode(self) -> bool:
        return self.target_path is not None


@dataclass
class ContainerPlan:
    container: ContainerInfo
    mount_plans: list[MountPlan] = field(default_factory=list)
    was_running: bool = False

    def active_plans(self) -> list[MountPlan]:
        return [p for p in self.mount_plans if not p.skip]

    def has_active_plans(self) -> bool:
        return any(not p.skip for p in self.mount_plans)


# ---------------------------------------------------------------------------
# Docker connection
# ---------------------------------------------------------------------------

def connect_docker(args: argparse.Namespace) -> docker.DockerClient:
    try:
        if args.host:
            tls_config = None
            if getattr(args, "tls_cert", None):
                tls_config = docker.tls.TLSConfig(
                    client_cert=(args.tls_cert, args.tls_key),
                    ca_cert=getattr(args, "tls_ca", None),
                    verify=bool(getattr(args, "tls_ca", None)),
                )
            client = docker.DockerClient(base_url=args.host, tls=tls_config)
        else:
            client = docker.from_env()
        client.ping()
        return client
    except docker.errors.DockerException as e:
        console.print(f"[red]Cannot connect to Docker:[/red] {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_containers(
    client: docker.DockerClient,
    filter_names: list[str] | None = None,
) -> list[ContainerInfo]:
    containers = client.containers.list(all=True)
    result = []
    for c in containers:
        name = c.name.lstrip("/")
        if filter_names and name not in filter_names:
            continue
        attrs = c.attrs
        raw_mounts = attrs.get("Mounts") or []
        container_mounts = []
        for m in raw_mounts:
            mtype = m.get("Type", "bind")
            if mtype == "bind":
                container_mounts.append(BindMount(
                    source=m.get("Source", ""),
                    destination=m["Destination"],
                    read_write=m.get("RW", True),
                    mode=m.get("Mode", "rw"),
                    propagation=m.get("Propagation", ""),
                    type="bind",
                ))
            elif mtype == "volume":
                container_mounts.append(BindMount(
                    source=m.get("Name", ""),
                    destination=m["Destination"],
                    read_write=m.get("RW", True),
                    mode=m.get("Mode", ""),
                    propagation="",
                    type="volume",
                ))
        labels = (attrs.get("Config") or {}).get("Labels") or {}
        result.append(ContainerInfo(
            id=attrs["Id"],
            name=name,
            state=attrs["State"]["Status"],
            image=attrs["Config"]["Image"],
            mounts=container_mounts,
            is_compose_managed="com.docker.compose.project" in labels,
            compose_project=labels.get("com.docker.compose.project"),
            compose_service=labels.get("com.docker.compose.service"),
            compose_config_files=labels.get("com.docker.compose.project.config_files"),
            compose_working_dir=labels.get("com.docker.compose.project.working_dir"),
        ))
    return result


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def _group_containers(containers: list[ContainerInfo]) -> list[tuple[str, list[ContainerInfo]]]:
    """Return [(group_label, [ContainerInfo, ...]), ...] ordered by first-seen."""
    seen: dict[str, list[ContainerInfo]] = {}
    for c in containers:
        key = c.compose_project if c.compose_project else c.name
        seen.setdefault(key, []).append(c)
    return list(seen.items())


def print_report(containers: list[ContainerInfo]) -> None:
    if not containers:
        console.print("[green]No containers found.[/green]")
        return

    groups = _group_containers(containers)

    table = Table(title="Docker Containers & Mounts", show_lines=True)
    table.add_column("Project / Container", style="cyan", no_wrap=True)
    table.add_column("Service", style="dim", no_wrap=True)
    table.add_column("State")
    table.add_column("Type", justify="center")
    table.add_column("Source", style="yellow")
    table.add_column("Mount Point", style="blue")
    table.add_column("RW", justify="center")

    for group_label, members in groups:
        # Collect all rows for this group first so we know total row count
        rows: list[tuple] = []
        for c in members:
            state_color = "green" if c.state == "running" else "dim"
            state_str = f"[{state_color}]{c.state}[/{state_color}]"
            service_str = c.compose_service or ""
            if c.mounts:
                for i, m in enumerate(c.mounts):
                    rw_str = "rw" if m.read_write else "[dim]ro[/dim]"
                    type_str = "[yellow]bind[/yellow]" if m.type == "bind" else "[blue]volume[/blue]"
                    rows.append((
                        service_str if i == 0 else "",
                        state_str if i == 0 else "",
                        type_str,
                        m.source,
                        m.destination,
                        rw_str,
                    ))
            else:
                rows.append((service_str, state_str, "[dim]—[/dim]", "[dim]no mounts[/dim]", "", ""))

        # Emit rows — project label only on the first row of the group
        for i, row in enumerate(rows):
            project_cell = f"[bold]{group_label}[/bold]" if i == 0 else ""
            table.add_row(project_cell, *row)

    console.print(table)
    total_groups = len(groups)
    total_containers = len(containers)
    if total_groups == total_containers:
        console.print(f"\nTotal: [bold]{total_containers}[/bold] container(s)\n")
    else:
        console.print(f"\nTotal: [bold]{total_groups}[/bold] stack(s) / [bold]{total_containers}[/bold] container(s)\n")


# ---------------------------------------------------------------------------
# Name / path helpers
# ---------------------------------------------------------------------------

def suggest_volume_name(container_name: str, dest_path: str, prefix: str = "") -> str:
    dest_part = dest_path.strip("/").replace("/", "_")
    raw = f"{prefix}{container_name}_{dest_part}" if prefix else f"{container_name}_{dest_part}"
    sanitized = re.sub(r"[^a-zA-Z0-9_.\-]", "_", raw)
    if sanitized and not sanitized[0].isalnum():
        sanitized = "v" + sanitized
    return sanitized[:63] or "volume"


def suggest_target_path(base_dir: str, container_name: str, dest_path: str, prefix: str = "") -> str:
    raw_container = f"{prefix}{container_name}" if prefix else container_name
    container_dir = re.sub(r"[^a-zA-Z0-9_.\-]", "_", raw_container)
    volume_dir = re.sub(r"[^a-zA-Z0-9_.\-]", "_", dest_path.strip("/").replace("/", "_"))
    return os.path.join(base_dir, container_dir, volume_dir)


def validate_volume_name(name: str) -> bool:
    return bool(name) and bool(re.match(r"^[a-zA-Z0-9][a-zA-Z0-9_.\-]*$", name)) and len(name) <= 63


# ---------------------------------------------------------------------------
# Mode selection
# ---------------------------------------------------------------------------

def resolve_migration_mode(args: argparse.Namespace) -> tuple[str, Optional[str]]:
    """Returns (mode, target_dir) where mode is 'volume' or 'directory'."""
    if args.target_dir:
        if not os.path.isdir(args.target_dir):
            console.print(f"[yellow]Warning: --target-dir '{args.target_dir}' does not exist or is not a directory.[/yellow]")
        return "directory", args.target_dir
    if args.yes:
        return "volume", None
    console.print("\n[bold]How would you like to migrate mounts?[/bold]")
    console.print("  [1] Named Docker volume    [dim](recommended — Docker manages storage in /var/lib/docker/volumes)[/dim]")
    console.print(
        "  [2] Relocate to host path  "
        "[dim](stores data at a new host path — use this for NFS or a specific disk)[/dim]"
    )
    console.print("  [3] Exit")
    console.print()
    console.print("  [dim]Note: named volumes cannot be stored at a custom path. If you need data on an NFS share\n  or a specific mount point, choose option 2 — it relocates the data to that path.[/dim]")
    choice = Prompt.ask("  Select", choices=["1", "2", "3"], default="1")
    if choice == "3":
        console.print("[yellow]Exiting.[/yellow]")
        sys.exit(0)
    if choice == "2":
        base = Prompt.ask("  Host path to migrate data into (e.g. /mnt/nfs)").strip()
        if not os.path.isdir(base):
            console.print(f"  [yellow]Warning: '{base}' does not exist or is not a directory.[/yellow]")
        return "directory", base
    return "volume", None


# ---------------------------------------------------------------------------
# Planning phase
# ---------------------------------------------------------------------------

def plan_container(
    c: ContainerInfo,
    args: argparse.Namespace,
    existing_volume_names: set[str],
    mode: str,
    target_dir: Optional[str],
) -> ContainerPlan:
    plan = ContainerPlan(container=c)

    if c.is_compose_managed:
        console.print(Panel(
            f"[yellow]Warning:[/yellow] This container is managed by Docker Compose.\n"
            f"Project: [bold]{c.compose_project}[/bold]  Service: [bold]{c.compose_service}[/bold]\n\n"
            f"After migration you should update your [cyan]docker-compose.yml[/cyan] (see suggested YAML below).",
            title="[yellow]Docker Compose Managed Container[/yellow]",
            border_style="yellow",
        ))

    prefix = getattr(args, "volume_prefix", "") or ""

    for m in c.mounts:
        if mode == "directory":
            suggested = suggest_target_path(target_dir, c.name, m.destination, prefix)

            if args.yes:
                target_path = suggested
                console.print(f"  [dim]Auto-selecting target path:[/dim] {target_path}")
            else:
                rw_label = "rw" if m.read_write else "ro"
                console.print(
                    f"\n  Mount ({m.type}): [yellow]{m.source}[/yellow] → [blue]{m.destination}[/blue] "
                    f"\\[{rw_label}]"
                )
                console.print(f"  Suggested target path: [bold]{suggested}[/bold]")
                answer = Prompt.ask(
                    "  Migrate? \\[y=yes / n=skip / custom-path]",
                    default="y",
                ).strip()
                if answer.lower() == "n":
                    plan.mount_plans.append(MountPlan(mount=m, volume_name="", skip=True))
                    continue
                if answer.lower() in ("y", "yes", ""):
                    target_path = suggested
                else:
                    target_path = answer

            plan.mount_plans.append(MountPlan(mount=m, volume_name="", target_path=target_path))

        else:  # volume mode
            suggested = suggest_volume_name(c.name, m.destination, prefix)
            candidate = suggested
            suffix = 1
            while candidate in existing_volume_names:
                candidate = f"{suggested}_{suffix}"
                suffix += 1

            if args.yes:
                volume_name = candidate
                console.print(f"  [dim]Auto-selecting volume name:[/dim] {volume_name}")
            else:
                rw_label = "rw" if m.read_write else "ro"
                console.print(
                    f"\n  Mount ({m.type}): [yellow]{m.source}[/yellow] → [blue]{m.destination}[/blue] "
                    f"\\[{rw_label}]"
                )
                console.print(f"  Suggested volume name: [bold]{candidate}[/bold]")
                answer = Prompt.ask(
                    "  Migrate? \\[y=yes / n=skip / custom-name]",
                    default="y",
                ).strip()
                if answer.lower() == "n":
                    plan.mount_plans.append(MountPlan(mount=m, volume_name=candidate, skip=True))
                    continue
                if answer.lower() in ("y", "yes", ""):
                    volume_name = candidate
                else:
                    if not validate_volume_name(answer):
                        console.print(f"  [red]Invalid volume name '{answer}'. Skipping.[/red]")
                        plan.mount_plans.append(MountPlan(mount=m, volume_name=candidate, skip=True))
                        continue
                    volume_name = answer

            existing_volume_names.add(volume_name)
            plan.mount_plans.append(MountPlan(mount=m, volume_name=volume_name))

    return plan


def build_all_plans(
    containers: list[ContainerInfo],
    args: argparse.Namespace,
    client: docker.DockerClient,
) -> list[ContainerPlan]:
    try:
        existing = {v.name for v in client.volumes.list()}
    except docker.errors.DockerException:
        existing = set()

    mode, target_dir = resolve_migration_mode(args)

    plans = []
    for c in containers:
        console.print(f"\n[bold cyan]--- Container: {c.name}[/bold cyan] ([dim]{c.state}[/dim])")
        plan = plan_container(c, args, existing, mode, target_dir)
        plans.append(plan)
    return plans


# ---------------------------------------------------------------------------
# Dry-run display
# ---------------------------------------------------------------------------

def show_dry_run(plans: list[ContainerPlan]) -> None:
    table = Table(title="[bold]Dry Run — Planned Actions[/bold]", show_lines=True)
    table.add_column("#", justify="right", style="dim")
    table.add_column("Action")
    table.add_column("Type")
    table.add_column("Target")
    table.add_column("Notes")

    step = 0
    for plan in plans:
        if not plan.has_active_plans():
            continue
        for mp in plan.active_plans():
            step += 1
            if mp.is_directory_mode:
                table.add_row(str(step), "[green]CREATE[/green]", "directory", mp.target_path, "on host")
            else:
                table.add_row(str(step), "[green]CREATE[/green]", "volume", mp.volume_name, "")
        step += 1
        running_note = "currently running" if plan.container.state == "running" else ""
        table.add_row(str(step), "[yellow]STOP[/yellow]", "container", plan.container.name, running_note)
        for mp in plan.active_plans():
            step += 1
            dest = mp.target_path if mp.is_directory_mode else mp.volume_name
            table.add_row(str(step), "[blue]COPY[/blue]", "data", f"{mp.mount.source} → {dest}", "via alpine:latest")
        step += 1
        table.add_row(str(step), "[red]REMOVE[/red]", "container", plan.container.name, "")
        step += 1
        table.add_row(str(step), "[green]CREATE[/green]", "container", plan.container.name, "with updated mount(s)")
        step += 1
        start_note = "" if plan.container.state == "running" else "was stopped — will not start"
        table.add_row(str(step), "[green]START[/green]", "container", plan.container.name, start_note)

    console.print(table)


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

def _check_target_dir_writable(plans: list[ContainerPlan]) -> bool:
    """Return True if all target base directories are writable; print errors and return False if not."""
    # Collect unique ancestor paths that must be writable (the deepest existing parent of each target)
    problem_dirs: set[str] = set()
    for plan in plans:
        for mp in plan.active_plans():
            if not mp.is_directory_mode:
                continue
            # Walk up to find the first existing ancestor and test write access
            check = mp.target_path
            while check and check != os.path.dirname(check):
                if os.path.exists(check):
                    if not os.access(check, os.W_OK):
                        problem_dirs.add(check)
                    break
                check = os.path.dirname(check)

    if not problem_dirs:
        return True

    console.print()
    console.print(Panel(
        "[red]Permission denied[/red] — the current user cannot write to the following path(s):\n\n"
        + "\n".join(f"  [yellow]{d}[/yellow]" for d in sorted(problem_dirs))
        + "\n\nFix with one of:\n"
        "  [cyan]sudo chown $USER " + " ".join(sorted(problem_dirs)) + "[/cyan]\n"
        "  [cyan]sudo chmod o+w " + " ".join(sorted(problem_dirs)) + "[/cyan]\n"
        "  or re-run this script with [cyan]sudo[/cyan]",
        title="[red]Pre-flight Check Failed[/red]",
        border_style="red",
    ))
    return False


# ---------------------------------------------------------------------------
# Migration engine
# ---------------------------------------------------------------------------

class Migrator:
    def __init__(self, client: docker.DockerClient, args: argparse.Namespace) -> None:
        self.client = client
        self.args = args

    def create_volume(self, name: str) -> bool:
        """Returns True if newly created, False if it already existed."""
        try:
            self.client.volumes.create(name=name)
            log.debug("Created volume %s", name)
            return True
        except docker.errors.APIError as e:
            if e.response is not None and e.response.status_code == 409:
                return False
            raise

    def stop_container(self, container: docker.models.containers.Container) -> None:
        timeout = getattr(self.args, "stop_timeout", 30)
        console.print(f"  Stopping container [cyan]{container.name}[/cyan]...", end=" ")
        container.stop(timeout=timeout)
        console.print("[green]OK[/green]")

    def copy_data(self, source: str, volume_name: str, source_type: str = "bind") -> None:
        image = getattr(self.args, "copy_image", "alpine:latest") or "alpine:latest"
        console.print(f"  Copying data [yellow]{source}[/yellow] → volume [bold]{volume_name}[/bold]...", end=" ")
        src_mount = (
            Mount(target="/src", source=source, type="volume", read_only=True)
            if source_type == "volume"
            else Mount(target="/src", source=source, type="bind", read_only=True)
        )
        output = self.client.containers.run(
            image=image,
            command=["sh", "-c", "cp -a /src/. /dst/ && echo COPY_OK"],
            mounts=[
                src_mount,
                Mount(target="/dst", source=volume_name, type="volume"),
            ],
            remove=True,
            stdout=True,
            stderr=True,
            user="root",
        )
        if b"COPY_OK" not in output:
            raise RuntimeError(f"Data copy failed. Output: {output.decode(errors='replace')[:500]}")
        console.print("[green]OK[/green]")

    def copy_data_to_dir(self, source: str, target_path: str, source_type: str = "bind") -> None:
        image = getattr(self.args, "copy_image", "alpine:latest") or "alpine:latest"
        os.makedirs(target_path, exist_ok=True)
        console.print(f"  Copying data [yellow]{source}[/yellow] → [bold]{target_path}[/bold]...", end=" ")
        os.chmod(target_path, 0o777)
        src_mount = (
            Mount(target="/src", source=source, type="volume", read_only=True)
            if source_type == "volume"
            else Mount(target="/src", source=source, type="bind", read_only=True)
        )
        output = self.client.containers.run(
            image=image,
            command=["sh", "-c", "cp -a /src/. /dst/ && echo COPY_OK"],
            mounts=[
                src_mount,
                Mount(target="/dst", source=target_path, type="bind"),
            ],
            remove=True,
            stdout=True,
            stderr=True,
            user="root",
        )
        if b"COPY_OK" not in output:
            raise RuntimeError(f"Data copy failed. Output: {output.decode(errors='replace')[:500]}")
        console.print("[green]OK[/green]")

    def extract_create_kwargs(
        self,
        container: docker.models.containers.Container,
        active_plans: list[MountPlan],
    ) -> dict:
        attrs = container.attrs
        cfg = attrs["Config"]
        hcfg = attrs["HostConfig"]

        vol_replace = {mp.mount.source: (mp.volume_name, mp.mount.type) for mp in active_plans if not mp.is_directory_mode}
        dir_replace = {mp.mount.source: (mp.target_path, mp.mount.type) for mp in active_plans if mp.is_directory_mode}
        mounts = self._build_mounts(attrs.get("Mounts") or [], vol_replace, dir_replace)

        log_config = None
        log_cfg_raw = hcfg.get("LogConfig") or {}
        if log_cfg_raw.get("Type") and log_cfg_raw["Type"] not in ("", "json-file"):
            log_config = LogConfig(
                type=log_cfg_raw["Type"],
                config=log_cfg_raw.get("Config") or {},
            )

        healthcheck = cfg.get("Healthcheck") or None
        # Build ports dict: {container_port: host_binding, ...}
        # Exposed ports with no binding are mapped to None (expose only).
        port_bindings = hcfg.get("PortBindings") or {}
        ports: dict = {}
        for cp in (cfg.get("ExposedPorts") or {}).keys():
            bindings = port_bindings.get(cp)
            if bindings:
                ports[cp] = [(b.get("HostIp", "") or "", b["HostPort"]) for b in bindings]
            else:
                ports[cp] = None

        kwargs: dict = dict(
            image=cfg["Image"],
            name=container.name.lstrip("/"),
            command=cfg.get("Cmd") or None,
            entrypoint=cfg.get("Entrypoint") or None,
            environment=cfg.get("Env") or [],
            labels=cfg.get("Labels") or {},
            hostname=cfg.get("Hostname") or None,
            user=cfg.get("User") or None,
            working_dir=cfg.get("WorkingDir") or None,
            stop_signal=cfg.get("StopSignal") or None,
            healthcheck=healthcheck,
            mounts=mounts,
            ports=ports or None,
            restart_policy=hcfg.get("RestartPolicy") or {},
            network_mode=hcfg.get("NetworkMode") or "bridge",
            volumes_from=hcfg.get("VolumesFrom") or None,
            cap_add=hcfg.get("CapAdd") or None,
            cap_drop=hcfg.get("CapDrop") or None,
            privileged=hcfg.get("Privileged", False),
            devices=hcfg.get("Devices") or None,
            shm_size=hcfg.get("ShmSize") or None,
            sysctls=hcfg.get("Sysctls") or None,
            ulimits=hcfg.get("Ulimits") or None,
            extra_hosts=hcfg.get("ExtraHosts") or None,
            group_add=hcfg.get("GroupAdd") or None,
            pid_mode=hcfg.get("PidMode") or None,
            ipc_mode=hcfg.get("IpcMode") or None,
            security_opt=hcfg.get("SecurityOpt") or None,
            log_config=log_config,
        )
        return {k: v for k, v in kwargs.items() if v is not None or k == "privileged"}

    def _build_mounts(
        self,
        attrs_mounts: list[dict],
        vol_replace: dict[str, tuple[str, str]],
        dir_replace: dict[str, tuple[str, str]],
    ) -> list[Mount]:
        result = []
        for m in attrs_mounts:
            mtype = m.get("Type", "bind")
            dest = m["Destination"]
            rw = m.get("RW", True)
            if mtype == "bind":
                src = m.get("Source", "")
                if src in vol_replace:
                    result.append(Mount(
                        target=dest,
                        source=vol_replace[src][0],
                        type="volume",
                        read_only=not rw,
                    ))
                elif src in dir_replace:
                    result.append(Mount(
                        target=dest,
                        source=dir_replace[src][0],
                        type="bind",
                        read_only=not rw,
                        propagation="rprivate",
                    ))
                else:
                    result.append(Mount(
                        target=dest,
                        source=src,
                        type="bind",
                        read_only=not rw,
                        propagation=m.get("Propagation") or "rprivate",
                    ))
            elif mtype == "volume":
                vol_name = m.get("Name", "")
                if vol_name in vol_replace:
                    result.append(Mount(
                        target=dest,
                        source=vol_replace[vol_name][0],
                        type="volume",
                        read_only=not rw,
                    ))
                elif vol_name in dir_replace:
                    result.append(Mount(
                        target=dest,
                        source=dir_replace[vol_name][0],
                        type="bind",
                        read_only=not rw,
                        propagation="rprivate",
                    ))
                else:
                    result.append(Mount(
                        target=dest,
                        source=vol_name,
                        type="volume",
                        read_only=not rw,
                    ))
            elif mtype == "tmpfs":
                result.append(Mount(target=dest, source="", type="tmpfs"))
        return result

    def reconnect_networks(
        self,
        new_container: docker.models.containers.Container,
        saved_attrs: dict,
    ) -> None:
        default_net = saved_attrs["HostConfig"].get("NetworkMode", "bridge")
        networks = (saved_attrs.get("NetworkSettings") or {}).get("Networks") or {}
        for net_name, net_cfg in networks.items():
            if net_name == default_net:
                continue
            try:
                network = self.client.networks.get(net_name)
                aliases = [
                    a for a in (net_cfg.get("Aliases") or [])
                    if a != new_container.id[:12]
                ]
                network.connect(new_container, aliases=aliases or None)
                log.debug("Reconnected network %s", net_name)
            except docker.errors.APIError as e:
                log.warning("Could not reconnect network %s: %s", net_name, e)

    def verify_result(self, mp: MountPlan) -> str:
        if mp.is_directory_mode:
            try:
                return "\n".join(os.listdir(mp.target_path)[:20])
            except OSError:
                return ""
        image = getattr(self.args, "copy_image", "alpine:latest") or "alpine:latest"
        output = self.client.containers.run(
            image=image,
            command=["sh", "-c", "ls /data | head -20"],
            mounts=[Mount(target="/data", source=mp.volume_name, type="volume", read_only=True)],
            remove=True,
            stdout=True,
            stderr=False,
        )
        return output.decode(errors="replace").strip()

    def rollback(
        self,
        plan: ContainerPlan,
        stage: str,
        created_volumes: list[str],
        created_dirs: list[str],
        saved_attrs: Optional[dict],
        container: Optional[docker.models.containers.Container],
    ) -> None:
        console.print(f"  [red]Rollback from stage '{stage}'...[/red]")
        try:
            if stage in ("container_recreated", "networks_connected"):
                console.print(
                    "  [yellow]Container was recreated. Migration is partially complete.\n"
                    "  Volumes/directories were NOT removed. Start the container manually if needed.[/yellow]"
                )
                return
            if stage == "container_removed" and saved_attrs:
                kwargs = self.extract_create_kwargs_from_attrs(saved_attrs, [])
                console.print("  Restoring original container config...")
                new_c = self.client.containers.create(**kwargs)
                self.reconnect_networks(new_c, saved_attrs)
                if plan.was_running:
                    new_c.start()
                console.print("  [yellow]Original container restored.[/yellow]")
            elif stage in ("data_copied", "container_stopped") and container:
                container.start()
                console.print("  Restarted stopped container.")
            for vname in created_volumes:
                try:
                    self.client.volumes.get(vname).remove()
                    log.debug("Removed volume %s during rollback", vname)
                except docker.errors.DockerException:
                    pass
            if created_dirs:
                console.print(
                    "  [yellow]These directories were created — review and remove manually if needed:[/yellow]\n"
                    + "\n".join(f"    {d}" for d in created_dirs)
                )
        except Exception as rb_err:  # pylint: disable=broad-exception-caught
            console.print(f"  [red]Rollback failed:[/red] {rb_err}")
            console.print(
                f"  [yellow]Manual cleanup may be needed.[/yellow] "
                f"Volumes created: {created_volumes}  Dirs created: {created_dirs}"
            )

    def extract_create_kwargs_from_attrs(self, saved_attrs: dict, active_plans: list) -> dict:
        class _FakeContainer:
            attrs = saved_attrs
            name = saved_attrs["Name"]

        return self.extract_create_kwargs(_FakeContainer(), active_plans)  # type: ignore[arg-type]

    def migrate_container(self, plan: ContainerPlan) -> bool:
        c = plan.container
        console.print(f"\n[bold]Migrating [cyan]{c.name}[/cyan]...[/bold]")

        created_volumes: list[str] = []
        created_dirs: list[str] = []
        stage = "start"
        container = None
        saved_attrs: Optional[dict] = None

        try:
            # Stage 1: create volumes / directories
            for mp in plan.active_plans():
                if mp.is_directory_mode:
                    console.print(f"  Creating directory [bold]{mp.target_path}[/bold]...", end=" ")
                    os.makedirs(mp.target_path, exist_ok=True)
                    created_dirs.append(mp.target_path)
                    console.print("[green]OK[/green]")
                else:
                    console.print(f"  Creating volume [bold]{mp.volume_name}[/bold]...", end=" ")
                    newly_created = self.create_volume(mp.volume_name)
                    created_volumes.append(mp.volume_name)
                    console.print("[green]OK[/green]" if newly_created else "[yellow]already exists, reusing[/yellow]")
            stage = "volumes_created"

            # Stage 2: stop container
            container = self.client.containers.get(c.id)
            plan.was_running = container.attrs["State"]["Running"]
            if plan.was_running:
                self.stop_container(container)
            stage = "container_stopped"

            # Stage 3: copy data
            if not getattr(self.args, "skip_copy", False):
                for mp in plan.active_plans():
                    if mp.is_directory_mode:
                        self.copy_data_to_dir(mp.mount.source, mp.target_path, mp.mount.type)
                    else:
                        self.copy_data(mp.mount.source, mp.volume_name, mp.mount.type)
            stage = "data_copied"

            # Stage 4: update compose file (before container removal so it's done even if recreation fails)
            if c.is_compose_managed:
                self._maybe_update_compose(c, plan.active_plans())

            # Stage 5: save config + remove
            container.reload()
            saved_attrs = copy.deepcopy(container.attrs)
            kwargs = self.extract_create_kwargs(container, plan.active_plans())
            console.print(f"  Removing container [cyan]{c.name}[/cyan]...", end=" ")
            container.remove()
            console.print("[green]OK[/green]")
            stage = "container_removed"

            # Stage 5: recreate
            console.print(f"  Recreating container [cyan]{c.name}[/cyan]...", end=" ")
            new_container = self.client.containers.create(**kwargs)
            console.print("[green]OK[/green]")
            stage = "container_recreated"

            # Stage 6: reconnect networks
            self.reconnect_networks(new_container, saved_attrs)
            stage = "networks_connected"

            # Stage 7: start if was running
            if plan.was_running:
                console.print(f"  Starting container [cyan]{c.name}[/cyan]...", end=" ")
                new_container.start()
                console.print("[green]OK[/green]")

            # Stage 8: verify
            if plan.active_plans():
                first_mp = plan.active_plans()[0]
                label = first_mp.target_path if first_mp.is_directory_mode else first_mp.volume_name
                console.print(f"  Verifying [bold]{label}[/bold]...", end=" ")
                listing = self.verify_result(first_mp)
                file_count = len(listing.splitlines())
                console.print(f"[green]OK[/green] ({file_count} top-level entries)")

            if c.is_compose_managed:
                self._print_compose_hint(c, plan.active_plans())

            return True

        except Exception as e:  # pylint: disable=broad-exception-caught
            console.print(f"\n  [red]Error during migration (stage={stage}):[/red] {e}")
            log.exception("Migration failed for %s at stage %s", c.name, stage)
            self.rollback(plan, stage, created_volumes, created_dirs, saved_attrs, container)
            return False

    def _print_compose_hint(self, c: ContainerInfo, active_plans: list[MountPlan]) -> None:
        volume_lines = []
        vol_names = []
        for mp in active_plans:
            if mp.is_directory_mode:
                volume_lines.append(f"      - {mp.target_path}:{mp.mount.destination}")
            else:
                volume_lines.append(f"      - {mp.volume_name}:{mp.mount.destination}")
                vol_names.append(f"  {mp.volume_name}:\n    external: true")

        yaml_snippet = (
            f"services:\n"
            f"  {c.compose_service}:\n"
            f"    volumes:\n"
            + "\n".join(volume_lines)
        )
        if vol_names:
            yaml_snippet += "\n\nvolumes:\n" + "\n".join(vol_names)

        console.print(Panel(
            f"[yellow]Update your docker-compose.yml for service '{c.compose_service}':[/yellow]\n\n"
            f"[cyan]{yaml_snippet}[/cyan]",
            title="[yellow]Compose File Update Required[/yellow]",
            border_style="yellow",
        ))

    def _maybe_update_compose(self, c: ContainerInfo, active_plans: list[MountPlan]) -> None:
        if getattr(self.args, "no_update_compose", False):
            return
        compose_file = _find_compose_file(c)
        if not compose_file:
            return
        if not self.args.yes:
            if not Confirm.ask(
                f"\n  Auto-update [cyan]{compose_file}[/cyan]?",
                default=True,
            ):
                return
        console.print(f"  Updating [cyan]{compose_file}[/cyan]...")
        update_compose_file(c, active_plans)


# ---------------------------------------------------------------------------
# Compose file updater
# ---------------------------------------------------------------------------

def _find_compose_file(c: ContainerInfo) -> Optional[str]:
    if not c.compose_config_files:
        return None
    for path in c.compose_config_files.split(","):
        path = path.strip()
        if path and os.path.isfile(path):
            return path
    return None


def _resolve_bind_source(raw: str, working_dir: Optional[str]) -> str:
    if os.path.isabs(raw):
        return os.path.normpath(raw)
    base = working_dir or os.getcwd()
    return os.path.normpath(os.path.join(base, raw))


def update_compose_file(
    c: ContainerInfo,
    active_plans: list,
) -> bool:
    compose_file = _find_compose_file(c)
    if not compose_file:
        console.print(
            f"  [yellow]Could not locate compose file for project "
            f"'{c.compose_project}' — update it manually.[/yellow]"
        )
        return False

    try:
        from ruamel.yaml import YAML, comments as ruamel_comments
    except ImportError:
        console.print(
            "  [yellow]ruamel.yaml is not installed — cannot auto-update compose file.[/yellow]\n"
            "  [dim]Install with: pip install ruamel.yaml[/dim]"
        )
        return False

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 4096

    with open(compose_file, encoding="utf-8") as fh:
        data = yaml.load(fh)

    services = data.get("services") or {}
    service = services.get(c.compose_service)
    if service is None:
        console.print(
            f"  [yellow]Service '{c.compose_service}' not found in {compose_file}.[/yellow]"
        )
        return False

    path_to_vol = {mp.mount.source: mp.volume_name for mp in active_plans if not mp.is_directory_mode}
    path_to_dir = {mp.mount.source: mp.target_path for mp in active_plans if mp.is_directory_mode}
    working_dir = c.compose_working_dir

    volumes = service.get("volumes") or []
    new_volumes = []
    updated_count = 0

    for vol in volumes:
        if isinstance(vol, str):
            parts = vol.split(":")
            if len(parts) >= 2:
                resolved = _resolve_bind_source(parts[0], working_dir)
                if resolved in path_to_vol:
                    new_volumes.append(":".join([path_to_vol[resolved]] + parts[1:]))
                    updated_count += 1
                    continue
                if resolved in path_to_dir:
                    new_volumes.append(":".join([path_to_dir[resolved]] + parts[1:]))
                    updated_count += 1
                    continue
        elif isinstance(vol, dict) and vol.get("type") in ("bind", "volume"):
            src = vol.get("source", "")
            resolved = _resolve_bind_source(src, working_dir) if vol.get("type") == "bind" else src
            if resolved in path_to_vol:
                new_vol = ruamel_comments.CommentedMap()
                new_vol["type"] = "volume"
                new_vol["source"] = path_to_vol[resolved]
                new_vol["target"] = vol.get("target", vol.get("destination", ""))
                if not vol.get("read_only", False) is False:
                    new_vol["read_only"] = vol["read_only"]
                new_volumes.append(new_vol)
                updated_count += 1
                continue
            if resolved in path_to_dir:
                new_vol = ruamel_comments.CommentedMap()
                new_vol["type"] = "bind"
                new_vol["source"] = path_to_dir[resolved]
                new_vol["target"] = vol.get("target", vol.get("destination", ""))
                if not vol.get("read_only", False) is False:
                    new_vol["read_only"] = vol["read_only"]
                new_volumes.append(new_vol)
                updated_count += 1
                continue
        new_volumes.append(vol)

    if updated_count == 0:
        console.print(
            f"  [yellow]No matching mount entries found in {compose_file} "
            f"for service '{c.compose_service}' — check paths and update manually.[/yellow]"
        )
        return False

    service["volumes"] = new_volumes

    # Top-level volumes block only needed for named volume mode
    named_vol_plans = [mp for mp in active_plans if not mp.is_directory_mode]
    if named_vol_plans:
        if "volumes" not in data:
            data["volumes"] = ruamel_comments.CommentedMap()
        for mp in named_vol_plans:
            if mp.volume_name not in data["volumes"]:
                vol_def = ruamel_comments.CommentedMap()
                vol_def["external"] = True
                data["volumes"][mp.volume_name] = vol_def

    backup = compose_file + ".bak"
    shutil.copy2(compose_file, backup)
    console.print(f"  Backup saved to [dim]{backup}[/dim]")

    with open(compose_file, "w", encoding="utf-8") as fh:
        yaml.dump(data, fh)

    console.print(f"  [green]Updated {compose_file}[/green]")
    return True


# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------

_CURRENT_PLAN: Optional[ContainerPlan] = None


def _signal_handler(_sig: int, _frame) -> None:
    console.print("\n[red]Interrupted. Exiting cleanly...[/red]")
    sys.exit(130)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migrate Docker bind mounts to named volumes or a target directory.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all bind mounts (read-only)
  python docker_volume_migrate.py --list

  # Dry run — show what would happen
  python docker_volume_migrate.py --dry-run

  # Migrate interactively (prompts for mode: named volume or directory)
  python docker_volume_migrate.py -c mycontainer

  # Migrate to an NFS mount point
  python docker_volume_migrate.py --target-dir /mnt/nfs

  # Migrate all containers to NFS, auto-confirm
  python docker_volume_migrate.py --target-dir /mnt/nfs --yes

  # Migrate all to named volumes, auto-confirm, with prefix
  python docker_volume_migrate.py --yes --volume-prefix prod_

  # Connect to a remote Docker host
  python docker_volume_migrate.py -H tcp://10.0.0.1:2376
""",
    )
    p.add_argument("-H", "--host", metavar="URL", help="Docker socket or URL")
    p.add_argument("--tls-cert", metavar="PATH", help="TLS client certificate")
    p.add_argument("--tls-key", metavar="PATH", help="TLS client key")
    p.add_argument("--tls-ca", metavar="PATH", help="TLS CA certificate")

    p.add_argument("-c", "--container", metavar="NAME", action="append", dest="containers",
                   help="Limit to container(s) by name (repeatable)")
    p.add_argument("-l", "--list", action="store_true", help="List all container mounts and exit")
    p.add_argument("-n", "--dry-run", action="store_true", help="Show planned actions, make no changes")
    p.add_argument("-y", "--yes", action="store_true", help="Auto-confirm all prompts")
    p.add_argument("--target-dir", metavar="PATH",
                   help="Migrate data into subdirectories of PATH (e.g. /mnt/nfs) instead of named volumes")
    p.add_argument("--skip-copy", action="store_true", help="Create empty volume/directory (do not copy data)")
    p.add_argument("--volume-prefix", metavar="STR", default="", help="Prefix for auto-generated volume/directory names")
    p.add_argument("--copy-image", metavar="IMAGE", default="alpine:latest",
                   help="Image for data copy helper (default: alpine:latest)")
    p.add_argument("--stop-timeout", metavar="SECONDS", type=int, default=30,
                   help="Container stop timeout in seconds (default: 30)")
    p.add_argument("--no-update-compose", action="store_true",
                   help="Skip automatic docker-compose.yml update after migration")
    p.add_argument("--log-file", metavar="PATH", help="Write log to file")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    level = logging.DEBUG if args.verbose else logging.WARNING
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file))
    logging.basicConfig(level=level, handlers=handlers,
                        format="%(asctime)s %(levelname)s %(message)s")

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    client = connect_docker(args)
    version = client.version().get("Version", "unknown")
    console.print(f"Connected to Docker daemon (version [bold]{version}[/bold])")

    with console.status("Scanning containers..."):
        containers = discover_containers(client, filter_names=args.containers)

    console.print(f"Found [bold]{len(containers)}[/bold] container(s) with mounts.\n")

    if not containers:
        return

    print_report(containers)

    if args.list:
        return

    plans = build_all_plans(containers, args, client)
    active_plans = [p for p in plans if p.has_active_plans()]

    if not active_plans:
        console.print("\n[green]Nothing to migrate.[/green]")
        return

    console.print("\n[bold]Migration Plan:[/bold]")
    summary = Table(show_lines=True)
    summary.add_column("Container", style="cyan")
    summary.add_column("Source", style="yellow")
    summary.add_column("Destination", style="green")
    for plan in active_plans:
        for i, mp in enumerate(plan.active_plans()):
            dest = mp.target_path if mp.is_directory_mode else mp.volume_name
            summary.add_row(
                plan.container.name if i == 0 else "",
                mp.mount.source,
                dest,
            )
    console.print(summary)

    if args.dry_run:
        console.print()
        show_dry_run(active_plans)
        console.print("\n[yellow]Dry run complete — no changes made.[/yellow]")
        return

    if not _check_target_dir_writable(active_plans):
        sys.exit(1)

    if not args.yes:
        if not Confirm.ask(
            f"\nProceed with [bold]{len(active_plans)}[/bold] container migration(s)?",
            default=False,
        ):
            console.print("[yellow]Aborted.[/yellow]")
            return

    migrator = Migrator(client, args)
    succeeded = 0
    failed = 0
    skipped = len(plans) - len(active_plans)

    for i, plan in enumerate(active_plans, 1):
        console.rule(f"[{i}/{len(active_plans)}] {plan.container.name}")
        ok = migrator.migrate_container(plan)
        if ok:
            succeeded += 1
        else:
            failed += 1

    console.rule("Done")
    parts = []
    if succeeded:
        parts.append(f"[green]{succeeded} succeeded[/green]")
    if failed:
        parts.append(f"[red]{failed} failed[/red]")
    if skipped:
        parts.append(f"[dim]{skipped} skipped[/dim]")
    console.print("  " + "  |  ".join(parts))


if __name__ == "__main__":
    main()
