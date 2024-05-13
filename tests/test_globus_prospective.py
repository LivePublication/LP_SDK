import json
import shutil
import tempfile
from functools import partial
from pathlib import Path

from lp_sdk.provenance import LpProvCrate
from lp_sdk.validation.schemas import provenance_crate_draft_schema
from lp_sdk.validation.validator import Validator


def parser(wep: dict, _input: dict) -> tuple[dict, dict]:
    """TDD: parse a WEP and input dict to generate the info needed for a prov crate"""
    step_info = {}
    param_links = {}
    return step_info, param_links


def test_globus_prospective():
    """
    TDD: generate a prospective crate from a WEP file
    This differs from test_provcrate.test_prov_crate_from_wep, which attempts to generate a prospective crate matching
    the example provenance crate, which was generated from CWL.
    """

    data_dir = Path(__file__).parent / 'data' / 'globus_prov'

    with tempfile.TemporaryDirectory() as d:
        d = Path(d)

        # Input WEP file
        input_wep = data_dir / 'WEP.json'
        input_wep = Path(shutil.copy(input_wep, d))

        # Input file
        input_path = data_dir / 'input.json'
        with open(input_path) as f:
            input_data = json.load(f)

        # Build crate from WEP file
        crate = LpProvCrate(d)
        crate.build_from_wep(input_wep, partial(parser, _input=input_data))

        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

    validator = Validator(provenance_crate_draft_schema)

    validator.validate(actual)