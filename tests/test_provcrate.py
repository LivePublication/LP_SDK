import json
import tempfile
from pathlib import Path

from provenance.crate import LpProvCrate


def _throw_or_print(msg, error=True):
    if error:
        raise AssertionError(msg)
    else:
        print(msg)


def _compare_dicts(d1, d2, name='root', error=True):
    for key in d1:
        if key not in d2:
            _throw_or_print(f"Key {key} not in {name}: {d2}", error)
            continue

        if isinstance(d1[key], dict):
            _compare_dicts(d1[key], d2[key], key, error)
        elif isinstance(d1[key], list):
            # Build dicts of items by id
            if not isinstance(d2[key], list):
                _throw_or_print(f"Item {d2[key]} is not a list", error)
                continue
            d1_items = {item['@id']: item for item in d1[key]}
            d2_items = {item['@id']: item for item in d2[key]}
            _compare_dicts(d1_items, d2_items, key, error)
        else:
            if d1[key] != d2[key]:
                _throw_or_print(f"{d1[key]} != {d2[key]}", error)


def test_create_prov_crate():
    with tempfile.TemporaryDirectory() as d:
        crate = LpProvCrate(d)
        # crate.add_file('tests/data/WEP.json')
        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            data = json.load(f)

    # Expected data
    with open('data/ro-crate-metadata.json') as f:
        expected = json.load(f)

    _compare_dicts(expected, data, error=False)
