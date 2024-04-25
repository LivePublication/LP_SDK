import json
import tempfile
from pathlib import Path

from lp_sdk.parser.retrospective import format_retro_rocrate, write_retro_rocrate
from lp_sdk.retrospective.crate import DistStepCrate
from lp_sdk.validation.util import CrateParts
from lp_sdk.validation.validator import Comparator


def test_create_retro_crate():
    """TDD: manual creation of the retrospective parts of the crate"""
    with tempfile.TemporaryDirectory() as d:
        out_file = Path(d) / 'ro-crate-metadata.json'

        crate = DistStepCrate(d)
        crate.write()

        assert out_file.exists()
        with open(Path(d) / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

    with open(Path(__file__).parent / 'data' / 'cwl_prov' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    comp = Comparator([CrateParts.retrospective],
                      expected)
    comp.compare(actual)


def test_retrospective():
    # This is more documentation that test, for now
    # Get data from the action provider on execution statues, e.g.:
    execution_data = {
        'runtime': 120,
        'platform': 'Ubuntu 20.04',
        'cpu': 'Intel(R) Xeon(R) CPU @ 2.30GHz',
        'memory': '8GB',
        'disk': '100GB',
    }

    crate = format_retro_rocrate(execution_data)
    # Write the crate to a file
    with tempfile.TemporaryDirectory() as d:
        out_file = Path(d) / 'ro_crate_metadata.json'
        assert not out_file.exists()

        write_retro_rocrate(crate, out_file)
        assert out_file.exists()

        with open(out_file) as f:
            result = f.read()

    # TODO: check contents
    print(result)


if __name__ == '__main__':
    test_retrospective()