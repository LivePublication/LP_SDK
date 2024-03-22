import json
import tempfile
from pathlib import Path

from provenance.crate import LpProvCrate


def _throw_or_print(msg, error=True, indent=0):
    if error:
        raise AssertionError(msg)
    else:
        print(' ' * indent + msg)


def _compare_dicts(d1, d2, name='root', error=True, indent=0):
    for key in d1:
        print(' ' * indent + key)
        if key not in d2:
            _throw_or_print(f"Key {key} not in {name}: {d2}", error, indent+2)
            continue

        if isinstance(d1[key], dict):
            _compare_dicts(d1[key], d2[key], key, error, indent+2)
        elif isinstance(d1[key], list):
            if key == '@graph':
                # Build dicts of items by id
                if not isinstance(d2[key], list):
                    _throw_or_print(f"Item {key}: {d2[key]} is not a list", error, indent+2)
                    continue
                d1_items = {item['@id']: item for item in d1[key]}
                d2_items = {item['@id']: item for item in d2[key]}
                _compare_dicts(d1_items, d2_items, key, error, indent+2)
            else:
                if d1[key] != d2[key]:
                    _throw_or_print(f"{d1[key]} != {d2[key]}", error, indent+2)
        else:
            if d1[key] != d2[key]:
                _throw_or_print(f"{d1[key]} != {d2[key]}", error, indent)


def test_create_prov_crate():
    """
    Testing/TDD of tooling to recreate the example provenance crate from https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate
    The code here will be very manual, used to understand the structure and logic needed in the provcrate implementation.
    """

    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        with open(d / 'packed.cwl', 'w+') as f:
            f.write('dummy data')

        crate = LpProvCrate(d)
        # Add conforms to statements
        profiles = [
            {'@id': 'https://w3id.org/ro/crate/1.1'},
            {'@id': 'https://w3id.org/workflowhub/workflow-ro-crate/1.0'}
        ]
        crate.crate.metadata['conformsTo'] = profiles

        # Root dataset conforms to provenance crate
        profiles = [
            {"@id": "https://w3id.org/ro/wfrun/process/0.1"},
            {"@id": "https://w3id.org/ro/wfrun/workflow/0.1"},
            {"@id": "https://w3id.org/ro/wfrun/provenance/0.1"},
            {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"}
        ]
        crate.crate.root_dataset['conformsTo'] = profiles

        # Add workflow file
        wf = crate.add_workflow(d / 'packed.cwl')

        p = crate.add_parameter('packed.cwl#main/input')
        p.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'defaultValue': 'file:///home/stain/src/cwltool/tests/wf/hello.txt',
                'encodingFormat': 'https://www.iana.org/assignments/media-types/text/plain',
                'name': 'main/input'
             }
        )
        # crate.add_file('tests/data/WEP.json')
        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            data = json.load(f)

    # Expected data
    with open(Path(__file__).parent / 'data' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    print()
    _compare_dicts(expected, data, error=False)
