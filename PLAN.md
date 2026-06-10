# Modular Refactor

- [x] Split `yunareada.py` into focused modules under `yuna/` with each file kept comfortably below the current monolith size.
- [x] Keep `yunareada.py` as a thin entrypoint/compatibility wrapper.
- [x] Update tests to import module owners directly where mutable globals moved.
- [x] Run ruff, pytest, py_compile, import/build smoke checks, and diff checks.
- [x] Review file sizes, imports, circular dependencies, and runtime deployment impact.
- [x] Redeploy/restart the `yunareada` container and verify scheduled ticks.
- [x] Update notes and clear pending/stale entries.
