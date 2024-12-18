from __future__ import annotations

import logging
import types
import typing as t
from collections import defaultdict
from collections.abc import Iterable

import gladier
import gladier.exc
import gladier.managers.compute_login_manager
import gladier.storage.config
import gladier.storage.migrations
import gladier.utils.automate
import gladier.utils.dynamic_imports
import gladier.utils.name_generation
import gladier.utils.tool_alias
import gladier.version
from gladier import GladierBaseTool
from gladier.client import GladierBaseClient
from gladier.managers import ComputeManager, FlowsManager
from gladier.managers.login_manager import (
    BaseLoginManager,
)
from gladier_tools.globus import Transfer

from lp_sdk.gladier import ProvenanceBaseTool
from lp_sdk.gladier.formal_parameters import FileFormalParameter
from lp_sdk.gladier.provenance_transfers import DistCrateTransfer

log = logging.getLogger(__name__)


class ProvenanceBaseClient(GladierBaseClient):
    """
    ProvenanceBaseClient is a specialized subclass of GladierBaseClient designed to handle
    the provenance of data in automated workflows. It can be extended with specific tools
    and functionalities that cater to the requirements of data provenance in a distributed
    environment.
    """
    orchestration_server_endpoint_id = None

    def __init__(
            self,
            auto_registration: bool = True,
            login_manager: t.Optional[BaseLoginManager] = None,
            flows_manager: t.Optional[FlowsManager] = None,
    ):
        self._tools = None
        self.storage = self._determine_storage()
        self.login_manager = login_manager or self._determine_login_manager(
            self.storage
        )

        self.flows_manager = flows_manager or FlowsManager(
            auto_registration=auto_registration, subscription_id=self.subscription_id
        )
        if self.globus_group:
            self.flows_manager.globus_group = self.globus_group
        if not self.flows_manager.flow_title:
            self.flows_manager.flow_title = f"{self.__class__.__name__} flow"

        self.compute_manager = ComputeManager(auto_registration=auto_registration)
        self.storage.update()

        for man in (self.flows_manager, self.compute_manager):
            man.set_storage(self.storage, replace=False)
            man.set_login_manager(self.login_manager, replace=False)
            man.register_scopes()

    @property
    def tools(self):
        """
        Override the GladierBaseClient tools property to automatically
        include provenance transfer steps for each provenance compute function
        """
        if getattr(self, "_tools", None):
            return self._tools

        gtools = getattr(self, "gladier_tools", [])
        if not gtools or not isinstance(gtools, Iterable):
            if not self.get_flow_definition():
                raise gladier.exc.ConfigException(
                    '"gladier_tools" must be a defined list of Gladier Tools. '
                    'Ex: ["gladier.tools.hello_world.HelloWorld"]'
                )

        # Insert dist crate transfers
        for gt in gtools:
            if isinstance(gt, types.FunctionType):
                for count, func in enumerate(gt.compute_functions):
                    log.debug(f"Adding provenance transfer step for {func.__name__}")
                    """ 
                    TODO: How do multiple compute functions executed as a 
                    single tool/step get handled? Dist Step Crate per function?
                    """
                    gtools.insert(gtools.index(gt) + count + 1, DistCrateTransfer(func.__name__))

        resolved_tools = [
            self.get_gladier_defaults_cls(gt, self.alias_class) for gt in gtools
        ]

        # Insert auto transfers
        auto_transfers = self.detect_transfers(resolved_tools)
        final_tools = []
        for tool in resolved_tools:
            if tool in auto_transfers:
                final_tools.extend(auto_transfers[tool])
            final_tools.append(tool)

        final_tools.extend(auto_transfers.get('out', []))

        self._tools = final_tools

        return self._tools


    def check_input(self, tool: GladierBaseTool, flow_input: dict):
        # Override normal behaviour, in order to check input is provided in expected location:
        # $.input.compute_fnc_name, rather than $.input
        def _chain_has_key(data: dict, key: str) -> bool:
            keys = key.split(".")
            for k in keys:
                if k not in data:
                    return False
                data = data[k]
            return True

        for req_input in tool.get_required_input():
            if not _chain_has_key(flow_input["input"], req_input):
                raise gladier.exc.ConfigException(
                    f'{tool} requires flow input value: "{req_input}"'
                )

    def get_formal_parameters(self) -> list[dict]:
        """
        Get the formal parameters for the flow definition.
        These identify the type/format of each parameter, and the functions to which they are inputs/outputs
        :return: A list of formal parameters
        """
        # Collect FPs from each tool
        tool_params = {}
        for tool in self.tools:
            if isinstance(tool, ProvenanceBaseTool):
                assert not any([k in tool_params for k in tool.get_required_input()]), (
                    "Function names should be unique across tools."
                )  # TODO: this is probably impractical - include tool name in key?
                tool_params.update(tool.parameter_mapping)

        # Get list of unique FPs
        unique_formal_params = []
        for key, params in tool_params.items():
            fps = params.get('args', []) + params.get('returns', [])
            for name, in_out, value in fps:
                # TODO: handle FPs that aren't of type FormalParameter
                if name not in unique_formal_params:
                    unique_formal_params.append(name)

        # Build out details + usage of each FP
        output = []
        for fp in unique_formal_params:
            details = {'name': fp.name, 'input': [], 'output': []}

            if isinstance(fp, FileFormalParameter):
                details['format'] = fp.format
                details['type'] = 'file'
            else:
                # TODO: validate that fp type matches arg type in signature (probably not here)
                details['type'] = fp.type.__name__

            # Identify which functions use this FP
            for func in tool_params.keys():
                usage = tool_params[func].get('args', []) + tool_params[func].get('returns', [])
                for name, in_out, value in usage:
                    if name == fp:
                        details[in_out].append(func)

            if len(details['input']) == 0:
                details.pop('input')
            if len(details['output']) == 0:
                details.pop('output')

            output.append(details)

        return output

    def detect_transfers(self, tools: list[GladierBaseTool]) -> dict[GladierBaseTool, list[GladierBaseTool]]:
        """
        Detect transfers between endpoints in the flow definition.
        :return: A list of transfer states
        """
        # Get inputs/outputs to states in the flow
        inputs = defaultdict(list)
        outputs = {}
        for tool in tools:
            if isinstance(tool, ProvenanceBaseTool):
                for param, func, path, local_path in tool.file_inputs:
                    inputs[param].append((tool, func, path, local_path))
                for param, func, path, local_path in tool.file_outputs:
                    outputs[param] = (tool, func, path, local_path)

        # Flow input is static data, so we create a new Transfer subclass for each auto transfer
        def _subclass_transfer(name):
            return type(name, (Transfer,), {'flow_input': {'transfer_sync_level': 'checksum'}})

        # Build a transfer for each file formal parameter that needs transfered
        transfers = defaultdict(list)
        for fp, uses in inputs.items():
            for tool, func, path, local_path in uses:
                # Transfer from one compute node to another
                if fp in outputs:
                    s_tool, s_func, s_path, s_local_path = outputs[fp]
                    alias = f'_auto_FP_{fp.name}_{s_func}_{func}'
                    transfer = _subclass_transfer(alias)(alias, gladier.utils.tool_alias.StateSuffixVariablePrefix)
                    # transfer = Transfer(alias, gladier.utils.tool_alias.StateSuffixVariablePrefix)
                    transfer.flow_input['transfer_source_path'] = s_local_path
                    transfer.flow_input['transfer_destination_path'] = local_path
                    transfer.flow_input['transfer_source_endpoint_id'] = s_tool.storage_id
                    transfer.flow_input['transfer_destination_endpoint_id'] = tool.storage_id
                    transfer.flow_input['transfer_recursive'] = False
                    transfers[tool].append(transfer)
                else:  # Transfer from orch node to compute (input)
                    alias = f'_auto_FP_{fp.name}_in_{func}'
                    transfer = _subclass_transfer(alias)(alias, gladier.utils.tool_alias.StateSuffixVariablePrefix)
                    transfer.flow_input['transfer_destination_path'] = local_path
                    transfer.flow_input['transfer_destination_endpoint_id'] = tool.storage_id
                    transfer.flow_input['transfer_source_endpoint_id'] = self.orchestration_server_endpoint_id
                    transfer.flow_input['transfer_source_path'] = local_path  # TODO: may not be what we want
                    transfer.flow_input['transfer_recursive'] = False
                    transfers[tool].append(transfer)

        # Transfer items that aren't used elsewhere to the orch node (output)
        for fp, (tool, func, path, local_path) in outputs.items():
            if fp not in inputs:
                alias = f'_auto_FP_{fp.name}_{func}_out'
                transfer = _subclass_transfer(alias)(alias, gladier.utils.tool_alias.StateSuffixVariablePrefix)
                transfer.flow_input['transfer_source_path'] = local_path
                transfer.flow_input['transfer_source_endpoint_id'] = tool.storage_id
                transfer.flow_input['transfer_destination_path'] = local_path
                transfer.flow_input['transfer_destination_endpoint_id'] = self.orchestration_server_endpoint_id
                transfer.flow_input['transfer_recursive'] = False
                transfers['out'].append(transfer)

        return transfers
