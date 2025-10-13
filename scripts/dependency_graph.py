"""Generate dependency graphs between internal dc43 packages."""

from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from pathlib import Path
from typing import Mapping, Sequence

import tomllib
from packaging.requirements import InvalidRequirement, Requirement

from _packages import PACKAGES

ROOT = Path(__file__).resolve().parents[1]


def _sanitise_node_name(name: str) -> str:
    return name.replace("-", "_")


def _extract_string_literals(node: ast.AST) -> set[str]:
    values: set[str] = set()
    if isinstance(node, ast.List | ast.Tuple | ast.Set):  # type: ignore[attr-defined]
        for element in node.elts:
            values.update(_extract_string_literals(element))
    elif isinstance(node, ast.Constant) and isinstance(node.value, str):
        values.add(node.value)
    elif isinstance(node, ast.Call):
        if isinstance(node.func, ast.Name) and node.func.id == "_dependency" and node.args:
            first_arg = node.args[0]
            if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                values.add(first_arg.value)
        for arg in node.args:
            values.update(_extract_string_literals(arg))
        for keyword in node.keywords:
            if keyword.value is not None:
                values.update(_extract_string_literals(keyword.value))
    elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        values.update(_extract_string_literals(node.left))
        values.update(_extract_string_literals(node.right))
    return values


def _parse_requirement(raw: str) -> str | None:
    try:
        requirement = Requirement(raw)
    except InvalidRequirement:
        candidate = raw.split("@")[0].strip()
        return candidate if candidate in PACKAGES else None
    name = requirement.name
    return name if name in PACKAGES else None


def _load_dc43_dependencies(setup_path: Path) -> tuple[set[str], set[str]]:
    tree = ast.parse(setup_path.read_text("utf-8"))
    required: set[str] = set()
    optional: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            if target.id == "_INTERNAL_CORE_DEPENDENCIES":
                required.update(
                    filter(lambda name: name in PACKAGES, _extract_string_literals(node.value))
                )
            elif target.id == "_OPTIONAL_INTERNAL_DEPENDENCIES":
                optional.update(
                    filter(lambda name: name in PACKAGES, _extract_string_literals(node.value))
                )
            elif target.id == "extras_require":
                extras_values = _extract_string_literals(node.value)
                optional.update(filter(None, (_parse_requirement(value) for value in extras_values)))
    return required, optional


def _read_pyproject_dependencies(pyproject: Path) -> tuple[set[str], set[str]]:
    data = tomllib.loads(pyproject.read_text("utf-8"))
    project: Mapping[str, object] = data.get("project", {})  # type: ignore[assignment]
    required: set[str] = set()
    optional: set[str] = set()
    dependencies: Sequence[str] = project.get("dependencies", [])  # type: ignore[assignment]
    for raw in dependencies:
        name = _parse_requirement(raw)
        if name:
            required.add(name)
    optional_dependencies: Mapping[str, Sequence[str]] = project.get(
        "optional-dependencies", {}
    )  # type: ignore[assignment]
    for items in optional_dependencies.values():
        for raw in items:
            name = _parse_requirement(raw)
            if name:
                optional.add(name)
    return required, optional


def _build_dependency_map() -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    required_edges: dict[str, set[str]] = defaultdict(set)
    optional_edges: dict[str, set[str]] = defaultdict(set)
    for package, metadata in PACKAGES.items():
        if package == "dc43":
            required, optional = _load_dc43_dependencies(ROOT / "setup.py")
        else:
            required, optional = _read_pyproject_dependencies(metadata["pyproject"])
        required_edges[package].update(required)
        optional_edges[package].update(optional - required)
    return required_edges, optional_edges


def render_mermaid() -> str:
    required_edges, optional_edges = _build_dependency_map()
    lines = ["graph TD"]
    for package in PACKAGES:
        node = _sanitise_node_name(package)
        lines.append(f"    {node}[\"{package}\"]")
    for source, targets in required_edges.items():
        src = _sanitise_node_name(source)
        for target in sorted(targets):
            dst = _sanitise_node_name(target)
            lines.append(f"    {src} --> {dst}")
    for source, targets in optional_edges.items():
        src = _sanitise_node_name(source)
        for target in sorted(targets):
            dst = _sanitise_node_name(target)
            lines.append(f"    {src} -.-> {dst}")
    lines.append("    %% Dashed arrows indicate optional extras")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the graph to the provided file instead of stdout",
    )
    args = parser.parse_args()
    graph = render_mermaid()
    if args.output:
        args.output.write_text(graph + "\n", "utf-8")
    else:
        print(graph)


if __name__ == "__main__":
    main()
