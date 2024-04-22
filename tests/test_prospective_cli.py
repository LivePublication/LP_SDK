import json
import tempfile
from pathlib import Path

from click.testing import CliRunner

from lp_sdk.parser.cli import prospective

TEST_DIR = Path(__file__).parent


def test_prospective():
    runner = CliRunner()
    in_file = TEST_DIR / 'data' / 'WEP.json'
    assert in_file.exists()
    with open(in_file) as f:
        wep = json.load(f)

    with tempfile.TemporaryDirectory() as d:
        out_file = Path(d) / 'ro_crate_metadata.json'
        assert not out_file.exists()

        # Equivelant to command line: lp-parser prospective -i in_file -o out_file
        runner.invoke(prospective, ['-i', in_file, '-o', out_file])
        assert out_file.exists()

        with open(out_file) as f:
            result = json.load(f)

    assert '@context' in result
    assert '@graph' in result
    for item in result['@graph']:
        assert '@id' in item
        assert '@type' in item

    item_ids = [item['@id'] for item in result['@graph']]
    for item in wep['States'].keys():
        assert item in item_ids
