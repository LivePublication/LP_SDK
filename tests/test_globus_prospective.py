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
    def _is_transfer(state_data: dict) -> bool:
        return (state_data.get('Type', '') == 'Action' and
                state_data.get('ActionUrl', '') == 'https://actions.globus.org/transfer/transfer')

    def _is_step_crate_transfer(state_name: str, state_data: dict) -> bool:
        return _is_transfer(state_data) and 'Transfer_provenance' in state_name

    def _iter_states(states: dict, start_at: str):
        state_name = start_at
        while state_name in states:
            yield state_name, states[state_name]
            state_name = states[state_name].get('Next', '')

    for state_name, state_data in _iter_states(wep):
        if _is_step_crate_transfer(state_name, state_data):
            # Step crate transfer - do nothing
            pass
        elif _is_transfer(state_data):
            # File transfer - use to map parameter connections
            pass
        else:
            # Compute state -
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
        crate.build_from_wep(input_wep, partial(parser, _input=input_data))

        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

    validator = Validator(provenance_crate_draft_schema)

    validator.validate(actual)