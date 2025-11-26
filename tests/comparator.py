# tests/comparator.py
from tests.config import FLOAT_TOLERANCE


class ComparisonResult:
    def __init__(self):
        self.errors = []

    def add_error(self, path, message):
        self.errors.append(f"Path '{path}': {message}")

    def is_success(self):
        return len(self.errors) == 0


def deep_compare(obj1, obj2, path="root", result=None):
    if result is None:
        result = ComparisonResult()

    # 1. Type Mismatch
    if type(obj1) != type(obj2):
        # Allow int vs float comparison if values are close
        if isinstance(obj1, (int, float)) and isinstance(obj2, (int, float)):
            pass
        else:
            result.add_error(path, f"Type mismatch. Local: {type(obj1)}, Remote: {type(obj2)}")
            return result

    # 2. Dictionaries
    if isinstance(obj1, dict):
        keys1 = set(obj1.keys())
        keys2 = set(obj2.keys())

        if keys1 != keys2:
            missing_in_local = keys2 - keys1
            missing_in_remote = keys1 - keys2
            if missing_in_local:
                result.add_error(path, f"Missing keys in Local: {missing_in_local}")
            if missing_in_remote:
                result.add_error(path, f"Extra keys in Local: {missing_in_remote}")
            return result  # Stop comparing this level if keys differ substantially

        for key in keys1:
            deep_compare(obj1[key], obj2[key], f"{path}.{key}", result)

    # 3. Lists
    elif isinstance(obj1, list):
        if len(obj1) != len(obj2):
            result.add_error(path, f"List length mismatch. Local: {len(obj1)}, Remote: {len(obj2)}")
            return result

        for i in range(len(obj1)):
            deep_compare(obj1[i], obj2[i], f"{path}[{i}]", result)

    # 4. Scalars (Floats/Ints/Strings)
    else:
        # Float comparison
        if isinstance(obj1, (float, int)):
            diff = abs(float(obj1) - float(obj2))
            if diff > FLOAT_TOLERANCE:
                result.add_error(path, f"Value mismatch. Local: {obj1}, Remote: {obj2} (Diff: {diff})")
        # String/Bool comparison
        elif obj1 != obj2:
            result.add_error(path, f"Value mismatch. Local: '{obj1}', Remote: '{obj2}'")

    return result