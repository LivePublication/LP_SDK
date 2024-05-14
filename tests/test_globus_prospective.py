import json
import shutil
import tempfile
from collections import defaultdict
from functools import partial
from pathlib import Path

from lp_sdk.provenance import LpProvCrate
from lp_sdk.validation.schemas import provenance_crate_draft_schema
from lp_sdk.validation.validator import Validator

from lp_sdk.parser import wep_parsing


def parser(wep: dict, input_: dict, orch_epid: str) -> tuple[dict, dict]:
    """TDD: parse a WEP and input dict to generate the info needed for a prov crate"""
    # Parse WEP into state objects
    compute_states, transfer_states = wep_parsing.parse_states(wep, input_)

    # First step through the WEP and simplify
    step_info = defaultdict(dict)
    param_links = []

    for transfer in transfer_states:
        if transfer.source_endpoint.value == orch_epid:
            # Transfer from orchestration endpoint
            step_info['main'].setdefault('input', []).extend(
                [t.source_path.value for t in transfer.transfer_items])
        elif transfer.destination_endpoint.value == orch_epid:
            # Transfer to orchestration endpoint
            step_info['main'].setdefault('output', []).extend(
                [t.destination_path.value for t in transfer.transfer_items])
        else:
            # Transfer between compute endpoints
            pass

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
        crate.build_from_wep(input_wep, partial(parser, input_=input_data, orch_epid='b782400e-3e59-412c-8f73-56cd0782301f'))

        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

    validator = Validator(provenance_crate_draft_schema)

    validator.validate(actual)