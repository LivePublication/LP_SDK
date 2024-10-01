import json
from pathlib import Path
from pprint import pprint

from gladier import generate_flow_definition
from lp_sdk.gladier.provenance_client import ProvenanceBaseClient
from lp_sdk.gladier.provenance_tool import ProvenanceBaseTool

# Flow configuration details
exp_compute_endpoint_uuid = 'e8c8b86f-a6fb-4c5b-9bc0-45423336f0c5'
exp_compute_GCS_uuid = 'abe41251-eb3c-4cc5-b329-cf9fe8fadcf1'
o_server_GCS_uuid = '4a420ad5-4113-4d7d-aba2-883f8208e897'
data_store_uuid = 'b782400e-3e59-412c-8f73-56cd0782301f'

orch_input_file = '/input/test.txt'
orch_output_file = '/output/test.txt'

rev_input_file = '/rev_text/input/test.txt'
rev_output_file = '/rev_text/output/test.txt'

sort_input_file = '/sort_text/input/test.txt'
sort_output_file = '/sort_text/output/test.txt'

ToCompute_source_uuid = data_store_uuid
ToCompute_dest_uuid = exp_compute_GCS_uuid
ToCompute_source_path = orch_input_file
ToCompute_dest_path = rev_input_file
ToCompute_recursive = False

RT_ST_source_uuid = exp_compute_GCS_uuid
RT_ST_dest_uuid = exp_compute_GCS_uuid
RT_ST_source_path = rev_output_file
RT_ST_dest_path = sort_input_file
RT_ST_recursive = False

FromCompute_source_uuid = exp_compute_GCS_uuid
FromCompute_dest_uuid = o_server_GCS_uuid  # Where should this go?
FromCompute_source_path = sort_output_file
FromCompute_dest_path = orch_output_file
FromCompute_recursive = False


def rev_txt(input_file, output_file):
    with open(input_file, 'r') as f:
        lines = f.readlines()
    lines = [line.strip() for line in lines]
    lines = [line[::-1] for line in lines if line]

    from pathlib import Path
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        f.write('\n'.join(lines))


@generate_flow_definition()
class ReverseText(ProvenanceBaseTool):
    """Reverse each line in a text file"""

    compute_functions = [rev_txt]


def sort_txt(input_file: str, output_file: str, reverse: bool):
    with open(input_file, 'r') as f:
        lines = f.readlines()
    lines = [line.strip() for line in lines]
    lines = [line for line in lines if line]
    lines.sort()
    if reverse:
        lines = lines[::-1]

    from pathlib import Path
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w') as f:
        f.write('\n'.join(lines))


@generate_flow_definition()
class SortText(ProvenanceBaseTool):
    """Sort the lines in a text file"""

    compute_functions = [sort_txt]

    # Defaults
    flow_input = {
        'SortTxt': {'reverse': False}
    }


@generate_flow_definition
class client(ProvenanceBaseClient):
    """Workflow matching the provenance rocrate example"""
    gladier_tools = [
        "gladier_tools.globus.Transfer:ToCompute",
        ReverseText,
        "gladier_tools.globus.Transfer:RT_ST",
        SortText,
        "gladier_tools.globus.Transfer:FromCompute",
        ]

    globus_group = "9e155e5c-e011-11ee-a4a3-8fbdadf65a0b"


if __name__ == '__main__':
    folder = Path(__file__).parent / 'Provenance_ROCrate_Example'
    folder.mkdir(exist_ok=True)

    test_client = client()
    test_client.flows_manager.flow_title = 'Provenance ROCrate Example'

    # Write WEP.json
    flow_definition = test_client.get_flow_definition()
    output_file = folder / 'WEP.json'
    with open(output_file, 'w') as f:
        json.dump(flow_definition, f, indent=4)

    # Register flow
    test_client.sync_flow()
    test_client.flows_manager.register_flow()
    flow_id = test_client.flows_manager.get_flow_id()

    # Define input
    _input = {
        'input': {
            # Compute & provenance config
            'compute_endpoint': exp_compute_endpoint_uuid,
            'prov_compute_GCS_id': exp_compute_GCS_uuid,
            'orchestration_server_endpoint_id': o_server_GCS_uuid,
            '_provenance_crate_destination_directory': flow_id,

            # Transfer configs
            "to_compute_transfer_source_endpoint_id": ToCompute_source_uuid,
            "to_compute_transfer_destination_endpoint_id": ToCompute_dest_uuid,
            "to_compute_transfer_source_path": ToCompute_source_path,
            "to_compute_transfer_destination_path": ToCompute_dest_path,
            "to_compute_transfer_recursive": ToCompute_recursive,

            "rt_st_transfer_source_endpoint_id": RT_ST_source_uuid,
            "rt_st_transfer_destination_endpoint_id": RT_ST_dest_uuid,
            "rt_st_transfer_source_path": RT_ST_source_path,
            "rt_st_transfer_destination_path": RT_ST_dest_path,
            "rt_st_transfer_recursive": RT_ST_recursive,

            "from_compute_transfer_source_endpoint_id": FromCompute_source_uuid,
            "from_compute_transfer_destination_endpoint_id": FromCompute_dest_uuid,
            "from_compute_transfer_source_path": FromCompute_source_path,
            "from_compute_transfer_destination_path": FromCompute_dest_path,
            "from_compute_transfer_recursive": FromCompute_recursive,

            # Compute inputs
            'RevTxt': {   # Note that these are name from the function, not the tool, as tools may have multiple functions
                'input_file': f'.{rev_input_file}',
                'output_file': f'.{rev_output_file}',
            },
            'SortTxt': {
                'input_file': f'.{sort_input_file}',
                'output_file': f'.{sort_output_file}',
                'reverse': True,
            },
        }}

    # Check required inputs of tools are present in input
    # TODO: do something more with this information
    def _chain_get(data: dict, key: str):
        keys = key.split('.')
        for k in keys:
            data = data[k]
        return data

    for tool in test_client.tools:
        if len(tool.compute_functions) > 0:
            required = tool.get_required_input()
            for req in required:
                try:
                    val = _chain_get(_input['input'], req)
                    print(f"Required input {req} found in input with value {val}")
                except KeyError:
                    print(f"Required input {req} not found in input")

    # Combine default inputs (e.g.: function ids) with explicit inputs and save to file
    full_input = test_client.get_input()
    full_input['input'].update(_input['input'])
    output_file = folder / 'input.json'
    with open(output_file, 'w') as f:
        json.dump(full_input, f, indent=4)

    # TODO: run flow
    for tool in test_client.tools:
        test_client.check_input(tool, full_input)

    # Run flow
    flow = test_client.run_flow(flow_input=_input, label='provenance test flow')

    action_id = flow["action_id"]
    test_client.progress(action_id)
    pprint(test_client.get_status(action_id))
