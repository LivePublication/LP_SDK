import pytest
from gladier import generate_flow_definition, GladierBaseTool, GladierBaseClient
from gladier.exc import ConfigException

from lp_sdk.gladier.provenance_client import ProvenanceBaseClient
from lp_sdk.gladier.provenance_tool import ProvenanceBaseTool
from lp_sdk.gladier.provenance_transfers import DistCrateTransfer


def _gen_compute_func(name):
    def _noop(input, output):
        pass

    _noop.__name__ = name
    return _noop


def _compare_dicts(a, b, exclude: list = None):
    if exclude is None:
        exclude = []
    for k, v in a.items():
        if k in exclude:
            continue
        if isinstance(v, dict):
            _compare_dicts(v, b[k], exclude)
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    _compare_dicts(item, b[k][i], exclude)
                else:
                    assert item == b[k][i]
        else:
            assert v == b[k]


def test_provenance_tool_generates_with_input_path():
    # Test tool without alias generates payload using complete input
    @generate_flow_definition
    class BaseTool(GladierBaseTool):
        compute_functions = [_gen_compute_func('_noop'), _gen_compute_func('_noop2')]

    base_tool = BaseTool()
    base_flow = base_tool.get_flow_definition()
    for state, data in base_flow['States'].items():
        assert data['Parameters']['tasks'][0]['payload.$'] == '$.input'

    # Test that provenance tool generates payload using input keyed with state name
    @generate_flow_definition
    class ProvTool(ProvenanceBaseTool):
        compute_functions = [_gen_compute_func('_noop'), _gen_compute_func('_noop2')]

    prov_tool = ProvTool()
    prov_flow = prov_tool.get_flow_definition()
    for state, data in prov_flow['States'].items():
        assert data['Parameters']['tasks'][0]['payload.$'] == f'$.input.{state}'

    # Check that nothing else has changed
    _compare_dicts(base_flow, prov_flow, exclude=['payload.$'])


def test_input_checking():
    # Test that the ProvenanceBaseTool adds compute function inputs to required inputs,
    # and finds them when checking inputs
    @generate_flow_definition
    class ProvTool1(ProvenanceBaseTool):
        compute_functions = [_gen_compute_func('_noop1'), _gen_compute_func('_noop2')]

    @generate_flow_definition
    class ProvTool2(ProvenanceBaseTool):
        compute_functions = [_gen_compute_func('_noop3'), _gen_compute_func('_noop4')]

    @generate_flow_definition
    class ProvClient(ProvenanceBaseClient):
        gladier_tools = [
            ProvTool1,
            ProvTool2,
        ]

    input = {
        fname: {
            'input': None,
            'output': None,
        } for fname in ['Noop1', 'Noop2', 'Noop3', 'Noop4']
    }

    client = ProvClient()

    # Check correct input passes
    for tool in client.tools:
        if isinstance(tool, DistCrateTransfer):
            continue
        client.check_input(tool, {'input': input})

    # Check missing input fails
    for tool in client.tools:
        if isinstance(tool, DistCrateTransfer):
            continue
        with pytest.raises(ConfigException):
            client.check_input(tool, {'input': {}})

    # Check base gladier logic doesn't change
    @generate_flow_definition
    class BaseClient(GladierBaseClient):
        gladier_tools = [
            ProvTool1,
            ProvTool2,
        ]

    # Expect base client to fail finding inputs
    base_client = BaseClient()
    for tool in base_client.tools:
        with pytest.raises(ConfigException):
            base_client.check_input(tool, {'input': input})

    # Pass in input that should work, but isn't correctly formatted for other usage
    base_input = {}
    for fname in ['Noop1', 'Noop2', 'Noop3', 'Noop4']:
        base_input[f'{fname}.input'] = None
        base_input[f'{fname}.output'] = None

    for tool in base_client.tools:
        base_client.check_input(tool, {'input': base_input})
