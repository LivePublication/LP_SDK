from lp_sdk.validation.util import CrateParts, detect_crate_type


class Comparator:
    """Utility class for comparing two partial crates"""
    def __init__(self, parts_to_check: list[CrateParts], expected: dict):
        self.expected = expected
        self.graph_dict = {item['@id']: item for item in expected['@graph']}
        self.parts_to_check = parts_to_check

    def _consider_id(self, item: dict) -> bool:
        """Check if a given id is in the expected graph, and of a type we care about"""
        if not isinstance(item, dict):
            return True
        assert '@id' in item, f"Item {item} does not have an id"
        id = item['@id']

        if id not in self.graph_dict:
            return True

        return detect_crate_type(self.graph_dict[id]) in self.parts_to_check

    def _filter_list(self, items: list[dict]) -> list[dict]:
        """Filter out items that are not in the expected graph, or are of a type we do not care about"""
        return [item for item in items if self._consider_id(item)]

    def _compare_dicts(self, expected: dict, actual: dict, path: list) -> bool:
        """Compare two dictionaries, returns True if they match for the expected parts"""
        for key in expected:
            _path = [*path, key]
            if not self._consider_id(expected[key]):
                continue

            if key not in actual:
                # Special case - lists of items that are not parts we care about, will be missing rather than empty
                if isinstance(expected[key], list):
                    filtered = [item for item in expected[key] if self._consider_id(item)]
                    if len(filtered) == 0:
                        continue
                raise AssertionError(f'Path: {" | ".join(_path)}\nKey {key} not in actual: {actual}')

            if isinstance(expected[key], dict):
                self._compare_dicts(expected[key], actual[key], _path)
            elif isinstance(expected[key], list):
                if isinstance(expected[key][0], dict):
                    # List of dicts, compare each item by key
                    # Noting that these are most likely {'@id': ...} links
                    e_dict = {item['@id']: item for item in self._filter_list(expected[key])}
                    if isinstance(actual[key], list):
                        a_dict = {item['@id']: item for item in actual[key]}
                    else:
                        a_dict = {actual[key]['@id']: actual[key]}
                    self._compare_dicts(e_dict, a_dict, _path)
                else:
                    # Not a list of dicts - compare directly
                    assert expected[key] == actual[key], \
                        f'Path: {" | ".join(_path)}\nValue {key}: {expected[key]} does not match: {actual[key]}'
            else:
                # Not a dict or a list - compare values directly
                assert expected[key] == actual[key], \
                    f'Path: {" | ".join(_path)}\nValue {key}: {expected[key]} does not match: {actual[key]}'

        return True

    def compare(self, actual: dict) -> bool:
        """Compare two partial crates, returns True if they match for the expected parts"""
        assert actual['@context'] == self.expected['@context'], \
            "Contexts do not match\n{actual['@context']}\n{self.expected['@context']}"

        actual_graph = {item['@id']: item for item in actual['@graph']}
        for key in self.graph_dict:
            path = ['@graph', key]
            if not self._consider_id(self.graph_dict[key]):
                continue

            if key not in actual_graph:
                raise AssertionError(f'Path: {" | ".join(path)}\nKey {key} not in actual: {actual}')

            self._compare_dicts(self.graph_dict[key], actual_graph[key], path)

        return True
