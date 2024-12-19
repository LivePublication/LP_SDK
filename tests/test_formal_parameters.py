from pathlib import Path

from gladier import generate_flow_definition

from lp_sdk.gladier import ProvenanceBaseTool, ProvenanceBaseClient
from lp_sdk.gladier.formal_parameters import FormalParameter, FileFormalParameter


def func_a(a: int) -> int:
    return a


def func_b(a: int, b: str, c: str) -> None:
    pass


def func_c(b: str, c: str) -> None:
    pass


a = FormalParameter('a', int)
b = FormalParameter('b', int)
c = FileFormalParameter('c', 'txt')
d = FileFormalParameter('d', 'txt')
e = FileFormalParameter('e', 'txt')


@generate_flow_definition
class ToolA(ProvenanceBaseTool):
    compute_functions = [func_a]

    parameter_mapping = {
        'FuncA': {
            'args': [a.input()],
            'returns': [b.output()]
        }
    }


@generate_flow_definition
class ToolB(ProvenanceBaseTool):
    compute_functions = [func_b]

    parameter_mapping = {
        'FuncB': {
            'args': [b.input(), c.input('intput.txt'), d.output('output.txt')],
            'returns': []
        }
    }


@generate_flow_definition
class ToolC(ProvenanceBaseTool):
    compute_functions = [func_c]

    parameter_mapping = {
        'FuncC': {
            'args': [d.input('input.txt'), e.output('output.txt')],
            'returns': []
        }
    }


@generate_flow_definition
class Client(ProvenanceBaseClient):
    gladier_tools = [
        ToolA,
        ToolB,
        ToolC
    ]


def test_client_formal_param_gen(tmp_path: Path):
    """Expect ProvenanceBaseClient to generate formal parameters for tools"""
    client = Client()

    formal_params = client.get_formal_parameters()

    assert formal_params == [
        {'name': 'a', 'type': 'int', 'input': ['FuncA']},
        {'name': 'b', 'type': 'int', 'input': ['FuncB'], 'output': ['FuncA']},
        {'name': 'c', 'type': 'file', 'format': 'txt', 'input': ['FuncB']},
        {'name': 'd', 'type': 'file', 'format': 'txt', 'input': ['FuncC'], 'output': ['FuncB']},
        {'name': 'e', 'type': 'file', 'format': 'txt', 'output': ['FuncC']}
    ]


def test_client_gens_transfers():
    """Expect ProvenanceBaseClient to generate transfers to/from actions"""
    client = Client()
    flow_definition = client.get_flow_definition()

    # Extract states in order
    next_state = flow_definition['StartAt']
    states = []
    while next_state:
        states.append((next_state, flow_definition['States'][next_state]))
        next_state = states[-1][1].get('Next', None)

    # Check for expected states in order
    # TODO: this tests current behaviour, update to test expected behaviour
    assert states[0][0] == 'FuncA'
    assert states[1][0] == 'Transfer_provenance_FuncA'
    assert states[2][0] == 'Transfer_auto_FP_c_in_FuncB'
    assert states[3][0] == 'FuncB'
    assert states[4][0] == 'Transfer_provenance_FuncB'
    assert states[5][0] == 'Transfer_auto_FP_d_FuncB_FuncC'
    assert states[6][0] == 'FuncC'
    assert states[7][0] == 'Transfer_provenance_FuncC'
    assert states[8][0] == 'Transfer_auto_FP_e_FuncC_out'
