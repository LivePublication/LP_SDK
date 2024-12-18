import inspect

from gladier import generate_flow_definition

from lp_sdk.gladier import ProvenanceBaseTool, ProvenanceBaseClient


def func_a(a: int) -> int:
    return a


def func_b(a: int, b: str, c: str) -> None:
    pass


def func_c(b: str, c: str) -> None:
    pass


class FormalParameter:
    def __init__(self, name: str, type_: type):
        self.name = name
        self.type = type_

    def __repr__(self):
        return f'FormalParameter({self.name}, {self.type})'

    @property
    def input(self):
        return self, 'input'

    @property
    def output(self):
        return self, 'output'


class FileFormalParameter(FormalParameter):
    def __init__(self, name: str, format: str = None):
        super().__init__(name, str)
        self.format = format

    def __repr__(self):
        return f'FileFormalParameter({self.name}, {self.format})'


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
            'inputs': [a.input],
            'outputs': [],
            'returns': [b.output]
        }
    }


@generate_flow_definition
class ToolB(ProvenanceBaseTool):
    compute_functions = [func_b]

    parameter_mapping = {
        'FuncB': {
            'inputs': [b.input, c.input, d.output],
            'outputs': [],
            'returns': []
        }
    }


@generate_flow_definition
class ToolC(ProvenanceBaseTool):
    compute_functions = [func_c]

    parameter_mapping = {
        'FuncC': {
            'inputs': [d.input, e.input],
            'outputs': [],
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
    assert states[2][0] == 'FuncB'
    assert states[3][0] == 'Transfer_provenance_FuncB'
    assert states[4][0] == 'FuncC'
    assert states[5][0] == 'Transfer_provenance_FuncC'
