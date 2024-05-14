import json
import shutil
import tempfile
from collections import defaultdict
from functools import partial
from pathlib import Path

from lp_sdk.provenance import LpProvCrate
from lp_sdk.validation.schemas import provenance_crate_draft_schema
from lp_sdk.validation.validator import Validator

from pydantic import BaseModel

def _chain_get(data: dict, key: str):
    keys = key.split('.')
    for k in keys:
        data = data[k]
    return data


class TransferState:
    def __init__(self, data: dict):
        self.source_endpoint = data['Parameters']['source_endpoint_id.$']
        self.destination_endpoint = data['Parameters']['destination_endpoint_id.$']

        # TODO: assumes only one item is being transferred
        self.source_path = data['Parameters']['transfer_items'][0]['source_path.$']
        self.destination_path = data['Parameters']['transfer_items'][0]['destination_path.$']
        self.recursive = data['Parameters']['transfer_items'][0]['recursive.$']


class GlobusParameterConnection(BaseModel):
    source_ep: str
    source_path: str
    dest_ep: str
    dest_path: str

    @staticmethod
    def parse(transfer_state: TransferState, _input: dict):
        # TODO: do we need to handle recursive?
        return GlobusParameterConnection(
            source_ep=_chain_get(_input, transfer_state.source_endpoint),
            source_path=_chain_get(_input, transfer_state.source_path),
            dest_ep=_chain_get(_input, transfer_state.destination_endpoint),
            dest_path=_chain_get(_input, transfer_state.destination_path),
        )

def parser(wep: dict, _input: dict, orch_epid: str) -> tuple[dict, dict]:
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


    # First step through the WEP and simplify
    step_info = defaultdict(dict)
    param_links = []

    for state_name, state_data in _iter_states(wep):
        if _is_step_crate_transfer(state_name, state_data):
            # Step crate transfer - do nothing
            pass
        elif _is_transfer(state_data):
            # File transfer - use to map parameter connections
            t = TransferState(state_data)

            # Identify input/output files from workflow
            if _chain_get(_input, t.source_endpoint) == orch_epid:
                # Transfer from orchestration endpoint
                step_info['main'].setdefault('input', []).append(_chain_get(_input, t.source_path))
                param_links.append(GlobusParameterConnection.parse(t, _input))
            elif _chain_get(_input, t.destination_endpoint) == orch_epid:
                # Transfer to orchestration endpoint
                step_info['main'].setdefault('output', []).append(_chain_get(_input, t.destination_path))
                param_links.append(GlobusParameterConnection.parse(t, _input))
            else:
                # Transfer between compute endpoints
                param_links.append(GlobusParameterConnection.parse(t, _input))
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