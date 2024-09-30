import pytest
from gladier import generate_flow_definition, GladierBaseTool

from lp_sdk.gladier.provenance_tool import ProvenanceBaseTool


def _noop(input, output):
    pass


def _noop2(input, output):
    pass


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
        compute_functions = [_noop, _noop2]

    base_tool = BaseTool()
    base_flow = base_tool.get_flow_definition()
    for state, data in base_flow['States'].items():
        assert data['Parameters']['tasks'][0]['payload.$'] == '$.input'

    # Test that provenance tool generates payload using input keyed with state name
    @generate_flow_definition
    class ProvTool(ProvenanceBaseTool):
        compute_functions = [_noop, _noop2]

    prov_tool = ProvTool()
    prov_flow = prov_tool.get_flow_definition()
    for state, data in prov_flow['States'].items():
        assert data['Parameters']['tasks'][0]['payload.$'] == f'$.input.{state}'

    # Check that nothing else has changed
    _compare_dicts(base_flow, prov_flow, exclude=['payload.$'])
