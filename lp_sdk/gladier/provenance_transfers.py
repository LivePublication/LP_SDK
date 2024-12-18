from typing import Mapping, Any

from gladier_tools.globus import Transfer

from gladier.utils.name_generation import get_upper_camel_case
from gladier.utils.tool_alias import StateSuffixVariablePrefix


class DistCrateTransfer(Transfer):
    """
    Stub of a class responsible specifically for transferring Dist Step crates back to the orchestration server.
    """
    # TODO: have this handle the state augmentation currently in ProvenanceBaseClient.get_flow_definition()
    required_input = [
        'prov_compute_GCS_id',
        'orchestration_server_endpoint_id',  # TODO: these first two may be more dynamic?
        '_provenance_crate_destination_directory',
    ]
    alias_exempt = [
        'prov_compute_GCS_id',
        'orchestration_server_endpoint_id',
        '_provenance_crate_destination_directory',
    ]

    def __init__(self, func_name: str):
        func_name = get_upper_camel_case(func_name)
        alias = f'_provenance_{func_name}'  # TODO: should be camel case
        super().__init__(alias, StateSuffixVariablePrefix)
        # TODO: we're making assumptions about both the function name, and it's ResultPath
        #   this is currently necessary as we don't have access to the previous state in get_flow_definition()
        self.func_name = func_name

    def get_flow_definition(self) -> Mapping[str, Any]:
        flow_definition = super().get_flow_definition()
        flow_definition['Comment'] = f"Transfer Dist Step crate for {self.func_name} back to the orchestration server"
        for state_name, state_data in flow_definition['States'].items():
            state_data['ResultPath'] = f'$.{state_name}'
            transfer_parameters = state_data['Parameters']
            transfer_items = transfer_parameters['DATA']

            # Source and destination endpoints may be set dynamcially

            # Transfer items (Distriubted Step Crates) are always
            # named {task_id}.crate

            transfer_items[0].pop('recursive.$', None)
            transfer_items[0]['recursive'] = True

            transfer_items[0].pop('source_path.$', None)
            transfer_items[0]['source_path.='] = f"`$.{self.func_name}.details.results[0].task_id` + '.crate'"
            transfer_items[0].pop('destination_path.$', None)
            transfer_items[0]['destination_path.='] = f"`$.input._provenance_crate_destination_directory` + '/' + `$.{self.func_name}.details.results[0].task_id`"

            transfer_parameters['source_endpoint.$'] = '$.input.prov_compute_GCS_id'
            transfer_parameters['destination_endpoint.$'] = '$.input.orchestration_server_endpoint_id'

        return flow_definition