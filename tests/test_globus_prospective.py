import json
import shutil
import tempfile
from collections import defaultdict
from functools import partial
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from lp_sdk.provenance import LpProvCrate
from lp_sdk.validation.schemas import provenance_crate_draft_schema
from lp_sdk.validation.validator import Validator

from lp_sdk.parser import wep_parsing


class FormalParameter(BaseModel):
    name: str
    value: Any
    addtionalType: str | None
    input_to: str | None
    output_from: str | None


def parser(wep: dict, input_: dict, orch_epid: str) -> tuple[dict, dict]:
    """TDD: parse a WEP and input dict to generate the info needed for a prov crate"""
    # Parse WEP into state objects
    compute_states, transfer_states = wep_parsing.parse_states(wep, input_)

    # # Formal parameters
    formal_params = {}

    # Identify all function parameters (which may be inputs or outputs)
    for state in compute_states:
        for task in state.tasks:
            assert isinstance(task.payload.value, dict), "Expecting payload to be dict of kwargs to func"
            for key, value in task.payload.value.items():
                formal_params[f'{task.payload.key}.{key}'] = {
                    'step': state.name,
                    'name': f'{task.payload.key}.{key}',
                    'value': value,
                }

    # Check that all strings (i.e.: potential paths) are unique
    # TODO: relax this constraint
    # string_parameters = [p['value'] for p in formal_params.values() if isinstance(p['value'], str)]
    # assert len(string_parameters) == len(set(string_parameters)), "All paths must be unique"

    # Use transfer states to identify which parameters are inputs/outputs (and that they are files)
    for transfer in transfer_states:
        for transfer_item in transfer.transfer_items:
            # If file sent to/from compute endpoint, mark it as a file, and as an input/output
            if transfer.source_endpoint.value != orch_epid:
                path = transfer_item.source_path.value
                # Find matching formal parameters
                keys = [k for k, v in formal_params.items() if v['value'] == path]
                # TODO: handle non-unique paths?
                assert len(keys) == 1, f"Expected 1 match for path {path}, found {len(keys)}"
                key = keys[0]

                # Update formal parameters
                formal_params[key]['additionalType'] = 'File'
                formal_params[key]['output'] = True

            if transfer.destination_endpoint.value != orch_epid:
                path = transfer_item.destination_path.value
                # Find matching formal parameters
                keys = [k for k, v in formal_params.items() if v['value'] == path]

                # TODO: handle non-unique paths?
                assert len(keys) == 1, f"Expected 1 match for path {path}, found {len(keys)}"
                key = keys[0]

                # Update formal parameters
                formal_params[key]['additionalType'] = 'File'
                formal_params[key]['input'] = True

        # Identify types of all non-file parameters
        for key, param in formal_params.items():
            if 'additionalType' not in param:
                param['additionalType'] = type(param['value'])
                # If not a file, it must be a direct input to the func
                # TODO: consider passing of data from one func to another via ResultPath
                param['input'] = True

    # # Parameter connections
    orch_params = {}  # Additional parameters at the 'main' level
    param_links = []
    def _param_from_value(value: str):
        for k, v in formal_params.items():
            if v['additionalType'] == 'File' and v['value'] == value:
                return k

    # Use transfers to create parameter connections for files
    for transfer in transfer_states:
        for transfer_item in transfer.transfer_items:
            if transfer.source_endpoint.value == orch_epid:
                # Transfer from orchestration endpoint
                dest_key = _param_from_value(transfer_item.destination_path.value)
                source_key = f'main#{dest_key}'
                orch_params[source_key] = {
                    'name': source_key,
                    'value': transfer_item.source_path.value,
                    'additionalType': 'File',
                    'input': True,
                }
            elif transfer.destination_endpoint.value == orch_epid:
                # Transfer to orchestration endpoint
                source_key = _param_from_value(transfer_item.source_path.value)
                dest_key = f'main#{source_key}'
                orch_params[dest_key] = {
                    'name': dest_key,
                    'value': transfer_item.destination_path.value,
                    'additionalType': 'File',
                    'output': True,
                }
            else:
                source_key = _param_from_value(transfer_item.source_path.value)
                dest_key = _param_from_value(transfer_item.destination_path.value)

            param_links.append((source_key, dest_key))

    # Non-file parameters are also inputs to main
    for key, param in formal_params.items():
        if param['additionalType'] != 'File':
            orch_params[f'main#{key}'] = param
            param_links.append((key, f'main#{key}'))

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