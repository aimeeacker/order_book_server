from __future__ import annotations

import importlib.machinery
import importlib.util
import os
from pathlib import Path
from types import ModuleType
from typing import Optional, Sequence

__all__ = [
    "append_checkpoint",
    "compute_json",
    "compute_to_file",
    "get_dataset_dir",
    "set_dataset_dir",
]


def _candidate_paths() -> list[Path]:
    package_dir = Path(__file__).resolve().parent
    workspace_root = package_dir.parent
    env_path = os.environ.get("COMPUTE_L4_NATIVE_PATH")

    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path))

    search_dirs = [
        package_dir,
        workspace_root / "target" / "release",
        workspace_root / "target" / "debug",
    ]
    patterns = [
        "_compute_l4_native*.so",
        "libcompute_l4*.so",
    ]

    for directory in search_dirs:
        for pattern in patterns:
            candidates.extend(sorted(directory.glob(pattern)))

    seen: set[Path] = set()
    unique: list[Path] = []
    for path in candidates:
        resolved = path.resolve()
        if resolved not in seen and path.exists():
            seen.add(resolved)
            unique.append(path)
    return unique


def _load_native() -> ModuleType:
    module_name = "compute_l4._compute_l4_native"
    for path in _candidate_paths():
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            continue
        if not isinstance(spec.loader, importlib.machinery.ExtensionFileLoader):
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    searched = "\n".join(str(path) for path in _candidate_paths())
    raise ImportError(
        "could not locate compute_l4 native extension; build it with "
        "`cargo build -p compute_l4 --release` or set COMPUTE_L4_NATIVE_PATH.\n"
        f"Searched:\n{searched}"
    )


_NATIVE = _load_native()


def compute_json(
    input: str,
    include_users: bool = False,
    include_trigger_orders: bool = False,
    assets: Optional[Sequence[str]] = None,
) -> str:
    return _NATIVE.compute_json(
        input,
        include_users=include_users,
        include_trigger_orders=include_trigger_orders,
        assets=list(assets) if assets is not None else None,
    )


def compute_to_file(
    input: str,
    output: str,
    include_users: bool = False,
    include_trigger_orders: bool = False,
    assets: Optional[Sequence[str]] = None,
) -> None:
    _NATIVE.compute_to_file(
        input,
        output,
        include_users=include_users,
        include_trigger_orders=include_trigger_orders,
        assets=list(assets) if assets is not None else None,
    )


def append_checkpoint(
    input: str,
    output_dir: Optional[str] = None,
    include_users: bool = False,
    include_trigger_orders: bool = False,
    assets: Optional[Sequence[str]] = None,
) -> dict:
    return _NATIVE.append_checkpoint(
        input,
        output_dir=output_dir,
        include_users=include_users,
        include_trigger_orders=include_trigger_orders,
        assets=list(assets) if assets is not None else None,
    )


def get_dataset_dir() -> str:
    return _NATIVE.get_dataset_dir()


def set_dataset_dir(output_dir: Optional[str] = None) -> str:
    return _NATIVE.set_dataset_dir(output_dir=output_dir)
