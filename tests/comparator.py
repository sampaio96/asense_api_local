# tests/comparator.py
import math
from tests.config import FLOAT_TOLERANCE

# Threshold to switch from printing every field to printing a summary
BATCH_THRESHOLD = 60


class CompareStats:
    def __init__(self):
        self.errors = []
        self.matches = 0
        self.checked_fields = 0

    def add_error(self, path, msg):
        self.errors.append(f"[FAIL] {path}: {msg}")

    def add_match(self):
        self.matches += 1
        self.checked_fields += 1

    def is_success(self):
        return len(self.errors) == 0


def _log(msg, level=0):
    """Helper to print indented logs."""
    indent = "  " * level
    print(f"{indent}{msg}")


def deep_compare(local, remote, path="root", stats=None, level=0):
    if stats is None:
        stats = CompareStats()

    # 1. Handle None
    if local is None and remote is None:
        return stats
    if local is None or remote is None:
        stats.add_error(path, f"Null mismatch. Local: {local}, Remote: {remote}")
        return stats

    # 2. Type Check (allow int/float interoperability)
    t_loc = type(local)
    t_rem = type(remote)

    # Normalize types for comparison (int vs float)
    if isinstance(local, (int, float)) and isinstance(remote, (int, float)):
        pass  # Compatible
    elif t_loc != t_rem:
        stats.add_error(path, f"Type mismatch. Local: {t_loc.__name__}, Remote: {t_rem.__name__}")
        return stats

    # 3. DICTIONARIES
    if isinstance(local, dict):
        l_keys = set(local.keys())
        r_keys = set(remote.keys())

        # Check Keys
        if l_keys != r_keys:
            missing = r_keys - l_keys
            extra = l_keys - r_keys
            if missing: stats.add_error(path, f"Missing keys: {missing}")
            if extra: stats.add_error(path, f"Extra keys: {extra}")
            # We continue comparing the intersection

        common_keys = l_keys.intersection(r_keys)
        is_large = len(common_keys) > BATCH_THRESHOLD

        if is_large:
            _log(f"[BATCH] Comparing {len(common_keys)} keys in '{path}'...", level)
        else:
            _log(f"{{ object }} '{path}' ({len(common_keys)} keys)", level)

        # Recurse
        mismatches_in_batch = 0
        sorted_keys = sorted(list(common_keys))  # Sort for consistent checking order

        for key in sorted_keys:
            # If large, suppress recursion printing by passing a specialized flag or just logic
            # We handle verbosity here: if large, we don't increase level indentation or print success per item
            sub_level = level + 1 if not is_large else -1

            prev_err_count = len(stats.errors)
            deep_compare(local[key], remote[key], f"{path}.{key}", stats, sub_level)

            if len(stats.errors) > prev_err_count:
                mismatches_in_batch += 1

        if is_large:
            if mismatches_in_batch == 0:
                _log(f"[OK] All {len(common_keys)} keys in '{path}' matched perfectly.", level)
            else:
                _log(f"[FAIL] Found {mismatches_in_batch} errors inside '{path}'.", level)

    # 4. LISTS
    elif isinstance(local, list):
        if len(local) != len(remote):
            stats.add_error(path, f"Length mismatch. Local: {len(local)}, Remote: {len(remote)}")
            # Compare up to the shortest length

        limit = min(len(local), len(remote))
        is_large = limit > BATCH_THRESHOLD

        if is_large:
            _log(f"[BATCH] Comparing {limit} items in array '{path}'...", level)
        else:
            _log(f"[ array ] '{path}' ({limit} items)", level)

        mismatches_in_batch = 0

        for i in range(limit):
            sub_level = level + 1 if not is_large else -1

            prev_err_count = len(stats.errors)
            deep_compare(local[i], remote[i], f"{path}[{i}]", stats, sub_level)

            if len(stats.errors) > prev_err_count:
                mismatches_in_batch += 1

        if is_large:
            if mismatches_in_batch == 0:
                _log(f"[OK] All {limit} array items in '{path}' matched.", level)
            else:
                _log(f"[FAIL] Found {mismatches_in_batch} errors in array '{path}'.", level)

    # 5. SCALARS (Int, Float, String, Bool)
    else:
        is_match = False
        val_str = str(local)
        if len(val_str) > 50: val_str = val_str[:50] + "..."

        if isinstance(local, (int, float)):
            diff = abs(float(local) - float(remote))
            if diff <= FLOAT_TOLERANCE:
                is_match = True
            else:
                stats.add_error(path, f"Value mismatch. L:{local} != R:{remote} (Diff: {diff})")
        else:
            if local == remote:
                is_match = True
            else:
                stats.add_error(path, f"Value mismatch. L:'{local}' != R:'{remote}'")

        # Log Success only if visible (level >= 0)
        if is_match:
            stats.add_match()
            if level >= 0:
                _log(f"[OK] {path} = {val_str}", level)

    return stats