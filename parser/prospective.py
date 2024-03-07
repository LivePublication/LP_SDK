import json
from pathlib import Path

from root import ROOT_DIR


def load_wep(path: Path) -> dict:
    assert path.is_file(), f"Input file {path} does not exist"
    with open(path) as f:
        data = json.load(f)

    return data


def parse_wep_to_rocrate(wep: dict) -> dict:
    raise NotImplementedError()


def validate_rocrate(data: dict) -> bool:
    expected_fields = [
        '@context',
        '@graph',
    ]

    # todo: return false rather than error
    assert all([field in data for field in expected_fields])

    expected_graph_fields = [
        '@id', '@type'
    ]
    for item in data['@graph']:
        assert all([field in item for field in expected_graph_fields])

    return True


def write_rocrate(data: dict, path: Path) -> bool:
    assert not path.exists(), f"Output path {path} already exists"
    raise NotImplementedError()

    return False
