import copy
import inspect
from typing import Mapping, Any, List

from gladier import GladierBaseTool
from gladier.utils.flow_traversal import iter_flow
from gladier.utils.name_generation import get_compute_flow_state_name

from lp_sdk.gladier.formal_parameters import FileFormalParameter


class ProvenanceBaseTool(GladierBaseTool):
    parameter_mapping = {}
    storage_id = None

    def get_function_inputs(self) -> Mapping[str, type]:
        """
        Get the input parameters for each compute function in the tool.
        :return: A dict of function parameter names to their types
        """
        inputs = {}
        for func in self.compute_functions:
            fname = get_compute_flow_state_name(func)
            sig = inspect.signature(func)
            for param in sig.parameters.values():
                inputs[f'{fname}.{param.name}'] = param.annotation

        return inputs

    def get_required_input(self) -> List[str]:
        required = copy.deepcopy(super().get_required_input())

        # Add compute function parameters as required inputs
        # TODO: check if this conflicts with intended usage of required_inputs
        required.extend(self.get_function_inputs().keys())

        return required

    def get_flow_definition(self) -> Mapping[str, Any]:
        flow_definition = super().get_flow_definition()
        for state_name, state_data in iter_flow(flow_definition):
            if state_data['Parameters']['tasks'][0]['payload.$'] == '$.input':
                state_data['Parameters']['tasks'][0]['payload.$'] = f'$.input.{state_name}'
        return flow_definition

    def localise_path(self, tool_name, path):
        """
        Localise a path to a specific tool by prefixing the tool and function names.
        """
        # This primarily serves to prevent collisions when compute functions share a node
        return f'{self.__class__.__name__}/{tool_name}/{path}'

    @property
    def file_inputs(self):
        out = []
        for func, params in self.parameter_mapping.items():
            for param, in_out, value in params['args']:
                if isinstance(param, FileFormalParameter) and in_out == 'input':
                    out.append((param, func, value, self.localise_path(func, value)))

        return out

    @property
    def file_outputs(self):
        out = []
        for func, params in self.parameter_mapping.items():
            for param, in_out, value in params['args']:
                if isinstance(param, FileFormalParameter) and in_out == 'output':
                    out.append((param, func, value, self.localise_path(func, value)))

        return out
