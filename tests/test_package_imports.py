from __future__ import annotations

import importlib
import pkgutil


PACKAGE_NAMES = ("generator", "pipeline", "examples")


def test_every_project_package_module_is_importable() -> None:
    imported = set()
    for package_name in PACKAGE_NAMES:
        package = importlib.import_module(package_name)
        imported.add(package_name)
        for module_info in pkgutil.walk_packages(package.__path__, prefix=f"{package_name}."):
            importlib.import_module(module_info.name)
            imported.add(module_info.name)

    assert set(PACKAGE_NAMES).issubset(imported)
