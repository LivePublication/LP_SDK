import json
from pathlib import Path

import pytest

from lp_sdk.parser.wep_parsing import InputValue, Task, TransferItem, ComputeState, TransferState, parse_states


@pytest.fixture
def input_data():
    input_fname = Path(__file__).parent / 'data' / 'globus_prov' / 'input.json'
    with open(input_fname) as f:
        return json.load(f)


@pytest.fixture
def wep_data():
    wep_fname = Path(__file__).parent / 'data' / 'globus_prov' / 'WEP.json'
    with open(wep_fname) as f:
        return json.load(f)


def test_parse_task(input_data):
    t = Task.parse({
        "endpoint.$": "$.input.compute_endpoint",
        "function.$": "$.input.rev_txt_function_id",
        "payload.$": "$.input.RevTxt"
    }, input_data)

    assert t.endpoint == InputValue(key='$.input.compute_endpoint', value='58fb6f2d-ff78-4f39-9669-38c12d01f566')
    assert t.function == InputValue(key='$.input.rev_txt_function_id', value='60232236-4b25-4a92-84a2-0b8e30feaaa7')
    assert t.payload == InputValue(key='$.input.RevTxt', value={
        "input_file": "/rev_text/input/test.txt",
        "output_file": "/rev_text/output/test.txt"
    })


def test_parse_transfer_item(input_data):
    # Transfer between steps
    t = TransferItem.parse({
        "source_path.$": "$.input.rt_st_transfer_source_path",
        "destination_path.$": "$.input.rt_st_transfer_destination_path",
        "recursive.$": "$.input.rt_st_transfer_recursive"
    }, input_data)

    assert t.source_path == InputValue(key='$.input.rt_st_transfer_source_path', value='/rev_text/output/test.txt')
    assert t.destination_path == InputValue(key='$.input.rt_st_transfer_destination_path',
                                            value='/sort_text/input/test.txt')
    assert t.recursive == InputValue(key='$.input.rt_st_transfer_recursive', value=False)

    # Transfer to/from orch
    t = TransferItem.parse({
        "source_path.$": "$.input.to_compute_transfer_source_path",
        "destination_path.$": "$.input.to_compute_transfer_destination_path",
        "recursive.$": "$.input.to_compute_transfer_recursive"
    }, input_data)

    assert t.source_path == InputValue(key='$.input.to_compute_transfer_source_path', value='/input/test.txt')
    assert t.destination_path == InputValue(key='$.input.to_compute_transfer_destination_path',
                                            value='/rev_text/input/test.txt')
    assert t.recursive == InputValue(key='$.input.to_compute_transfer_recursive', value=False)

    # Crate transfer: .= not yet implemented
    with pytest.raises(NotImplementedError):
        t = TransferItem.parse({
            "recursive": True,
            "source_path.=": "`$.RevTxt.details.results[0].task_id` + '.crate'",
            "destination_path.=": "`$.input._provenance_crate_destination_directory` + '/' + `$.RevTxt.details.results[0].task_id`"
        }, input_data)


def test_parse_compute_state(input_data):
    # Compute state
    compute_state = {
            "Comment": None,
            "Type": "Action",
            "ActionUrl": "https://compute.actions.globus.org",
            "ExceptionOnActionFailure": False,
            "Parameters": {
                "tasks": [
                    {
                        "endpoint.$": "$.input.compute_endpoint",
                        "function.$": "$.input.rev_txt_function_id",
                        "payload.$": "$.input.RevTxt"
                    }
                ]
            },
            "ResultPath": "$.RevTxt",
            "WaitTime": 300,
            "Next": "Transfer_provenance_rev_txt"
        }

    c = ComputeState.parse('RevTxt', compute_state, input_data)

    assert c.name == 'RevTxt'
    assert c.comment is None
    assert c.resultPath == '$.RevTxt'
    assert len(c.tasks) == 1
    t = c.tasks[0]
    assert t.endpoint == InputValue(key='$.input.compute_endpoint', value='58fb6f2d-ff78-4f39-9669-38c12d01f566')
    assert t.function == InputValue(key='$.input.rev_txt_function_id', value='60232236-4b25-4a92-84a2-0b8e30feaaa7')
    assert t.payload == InputValue(key='$.input.RevTxt', value={
        "input_file": "/rev_text/input/test.txt",
        "output_file": "/rev_text/output/test.txt"
    })


def test_parse_transfer_state(input_data):
    transfer_state = {
            "Comment": "Transfer a file or directory in Globus",
            "Type": "Action",
            "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
            "Parameters": {
                "source_endpoint_id.$": "$.input.to_compute_transfer_source_endpoint_id",
                "destination_endpoint_id.$": "$.input.to_compute_transfer_destination_endpoint_id",
                "transfer_items": [
                    {
                        "source_path.$": "$.input.to_compute_transfer_source_path",
                        "destination_path.$": "$.input.to_compute_transfer_destination_path",
                        "recursive.$": "$.input.to_compute_transfer_recursive"
                    }
                ]
            },
            "ResultPath": "$.Transfer",
            "WaitTime": 600,
            "Next": "RevTxt"
        }

    t = TransferState.parse(transfer_state, input_data)

    assert t.source_endpoint == InputValue(key='$.input.to_compute_transfer_source_endpoint_id',
                                             value='b782400e-3e59-412c-8f73-56cd0782301f')
    assert t.destination_endpoint == InputValue(key='$.input.to_compute_transfer_destination_endpoint_id',
                                                  value='8ee44381-114a-45de-b8f8-d105a90c200d')
    assert len(t.transfer_items) == 1
    ti = t.transfer_items[0]
    assert ti.source_path == InputValue(key='$.input.to_compute_transfer_source_path', value='/input/test.txt')
    assert ti.destination_path == InputValue(key='$.input.to_compute_transfer_destination_path',
                                              value='/rev_text/input/test.txt')
    assert ti.recursive == InputValue(key='$.input.to_compute_transfer_recursive', value=False)


def test_parse_full_wep(input_data, wep_data):
    """Check that parsing the full WEP works without error"""
    compute_states, transfer_states = parse_states(wep_data, input_data)

    assert len(compute_states) == 2
    assert len(transfer_states) == 3
    assert compute_states[0].name == 'RevTxt'
    assert compute_states[1].name == 'SortTxt'
