import json
from collections import defaultdict
from pathlib import Path

from rocrate.model import ContextEntity, ComputationalWorkflow
from rocrate.rocrate import ROCrate
from runcrate import convert


# TODO:
# - add self-ref to crate, with conformsTo
# - add ref to base dataset
#   - conformsTo
#   - hasPart
#   - mainEntity: .wep?
#   - mentions: run of workflow?


def _strip_wep_param(param: str):
    return param[2:]


def _parse_wep(wep: dict, main_endpoint: str):
    """Parses states in a WEP file to determine order, extract parameters, and link parameters"""
    step = wep['StartAt']
    props = wep['States'][step]
    seen = {step}  # Check we don't get stuck in a loop

    rval = defaultdict(dict)
    links = []

    pos = 0
    while True:
        is_transfer = props['ActionUrl'] == 'https://actions.automate.globus.org/transfer/transfer'
        if is_transfer:
            # Link the output of one step to the input of another
            source_step = _strip_wep_param(props['Parameters']['source_endpoint_id.$'])
            if source_step == main_endpoint:
                source_step = 'main'

            dest_step = _strip_wep_param(props['Parameters']['destination_endpoint_id.$'])
            if dest_step == main_endpoint:
                dest_step = 'main'

            for transfers in props['Parameters']['transfer_items']:
                source_path = _strip_wep_param(transfers['source_path.$'])
                dest_path = _strip_wep_param(transfers['destination_path.$'])

                # Parameter name
                # TODO - this (using the actionUrl rather than step as id) is probably not actually what we want
                if source_step == 'main':
                    source_name = f'{source_step}/{source_path}'
                else:
                    tool_name = wep['States'][source_step]["ActionUrl"]
                    source_name = f'{tool_name}/{source_path}'
                if dest_step == 'main':
                    dest_name = f'{dest_step}/{dest_path}'
                else:
                    tool_name = wep['States'][dest_step]["ActionUrl"]
                    dest_name = f'{tool_name}/{dest_path}'

                # Transfers to/from main are input-input, output-output
                # Transfers between steps are output-input
                if source_step == 'main':
                    rval['main'].setdefault('input', []).append(source_name)
                else:
                    # Inputs from previous steps should already be defined
                    assert source_name in rval[source_step]['output'], \
                        f"Output parameter {source_name} not found for step {source_step}"
                    rval[source_step].setdefault('output', []).append(source_name)

                if dest_step == 'main':
                    rval['main'].setdefault('output', []).append(dest_name)
                else:
                    rval[dest_step].setdefault('input', []).append(dest_name)

                # Link parameters
                links.append((source_name, dest_name))

        else:  # Not a transfer step
            # Check that input parameters are already registered (by previous transfer step)
            tool_name = props["ActionUrl"]
            for input_param in props['Parameters'].values():
                input_name = f'{tool_name}/{_strip_wep_param(input_param)}'
                assert input_name in rval[step]['input'], \
                    f"Input parameter {input_param} not found for step {step}"

            # Register output parameters
            output_name = f'{tool_name}/{_strip_wep_param(props["ResultPath"])}'
            rval[step].setdefault('output', []).append(output_name)

            # Set position
            rval[step]['pos'] = pos
            pos += 1

        # Set next step
        if "Next" not in props or props.get("End", False):
            break
        step = props["Next"]
        assert step not in seen, f"Loop detected in WEP file at: {step}"
        seen.add(step)
        assert step in wep['States'], f"Step {step} not found in WEP file"
        props = wep['States'][step]

    return rval, links


class LpProvCrate:
    def __init__(self, path: str):
        self.path = Path(path)
        self.crate = ROCrate()

    def build_from_cwl(self, wf_file: Path):
        """Build a crate from a workflow file"""
        # Add rocrate profiles
        # TODO: don't hard-code these, get from somewhere
        profiles = [
            ("https://w3id.org/ro/wfrun/process/", "0.1", 'Process Run Crate'),
            ("https://w3id.org/ro/wfrun/workflow/", "0.1", 'Workflow Run Crate'),
            ("https://w3id.org/ro/wfrun/provenance/", "0.1", 'Provenance Run Crate'),
            ("https://w3id.org/workflowhub/workflow-ro-crate/", "1.0", 'Workflow RO-Crate'),
        ]
        profile_entities = [self.add_profile(f'{p[0]}{p[1]}', p[2], p[1]) for p in profiles]
        self.crate.root_dataset['conformsTo'] = profile_entities

        # TODO: everything below is a MVP re-implementation of runcrate.convert.ProvCrateBuilder
        #       this should serve as a skeleton from which to build the WEP implementation
        #       but should eventually be refactored so that we can handle both in full detail

        # Add workflow
        wf_defs = convert.get_workflow(wf_file)
        wf = self.add_workflow(wf_file)

        for input in wf_defs[wf.id].inputs:
            id = f'{input.id.split("#")[-1]}'
            input_ent = self.add_parameter(f'{wf.id}#{id}', id,
                                           convert.properties_from_cwl_param(input))
            wf.append_to('input', input_ent)

        for output in wf_defs[wf.id].outputs:
            id = f'{output.id.split("#")[-1]}'
            output_ent = self.add_parameter(f'{wf.id}#{id}', id,
                                            convert.properties_from_cwl_param(output))
            wf.append_to('output', output_ent)

        pos_map = convert.ProvCrateBuilder._get_step_maps(wf_defs)

        # Add steps
        for step in getattr(wf_defs[wf.id], 'steps', []):
            id = f'{step.id.split("#")[-1]}'
            pos = pos_map[wf.id][id]['pos']
            step_ent = self.add_step(f'{wf.id}#{id}', str(pos))
            wf.append_to('step', step_ent)

            # Add tools
            tool_id = f'{step.run.split("#")[-1]}'
            tool_ent = self.add_tool(f'{wf.id}#{tool_id}', tool_id, wf_defs[tool_id].doc)

            for input in wf_defs[tool_id].inputs:
                id = f'{input.id.split("#")[-1]}'
                input_ent = self.add_parameter(f'{wf.id}#{id}', id,
                                               convert.properties_from_cwl_param(input))
                tool_ent.append_to('input', input_ent)

            for output in wf_defs[tool_id].outputs:
                id = f'{output.id.split("#")[-1]}'
                output_ent = self.add_parameter(f'{wf.id}#{id}', id,
                                                convert.properties_from_cwl_param(output))
                tool_ent.append_to('output', output_ent)

            wf.append_to('hasPart', tool_ent)
            step_ent['workExample'] = tool_ent

    def build_from_wep(self, wep_file: Path):
        """Build a crate from a WEP file"""
        # TODO: don't hard-code these, get from somewhere
        profiles = [
            ("https://w3id.org/ro/wfrun/process/", "0.1", 'Process Run Crate'),
            ("https://w3id.org/ro/wfrun/workflow/", "0.1", 'Workflow Run Crate'),
            ("https://w3id.org/ro/wfrun/provenance/", "0.1", 'Provenance Run Crate'),
            ("https://w3id.org/workflowhub/workflow-ro-crate/", "1.0", 'Workflow RO-Crate'),
        ]
        profile_entities = [self.add_profile(f'{p[0]}{p[1]}', p[2], p[1]) for p in profiles]
        self.crate.root_dataset['conformsTo'] = profile_entities

        with open(wep_file, 'r') as f:
            wep = json.load(f)

        # Parse WEP file
        # TODO: use links to create formal parameters
        step_info, param_links = _parse_wep(wep, 'data_store_ep_id')

        # Add workflow
        wf = self.add_workflow(wep_file)

        param_props = {
            '@type': 'FormalParameter',
            'additionalType': 'File'
        }

        for input in step_info['main'].get('input', []):
            input_ent = self.add_parameter(f'{wf.id}#{input}', input, param_props)
            wf.append_to('input', input_ent)

        for output in step_info['main'].get('output', []):
            output_ent = self.add_parameter(f'{wf.id}#{output}', output, param_props)
            wf.append_to('output', output_ent)

        # Add steps
        for step_id, info in step_info.items():
            if step_id == 'main':
                continue  # Main represents the workflow, not a step
            step_ent = self.add_step(f'{wf.id}#main/{step_id}', str(info['pos']))
            wf.append_to('step', step_ent)

            # Add tool
            props = wep['States'][step_id]
            name = props["ActionUrl"]
            tool_ent = self.add_tool(f'{wf.id}#{name}', name, props['Comment'])

            for input in info['input']:
                input_ent = self.add_parameter(f'{wf.id}#{input}', input, param_props)
                tool_ent.append_to('input', input_ent)

            for output in info['output']:
                output_ent = self.add_parameter(f'{wf.id}#{output}', output, param_props)
                tool_ent.append_to('output', output_ent)

            wf.append_to('hasPart', tool_ent)
            step_ent['workExample'] = tool_ent

    def add_workflow(self, file: Path) -> ComputationalWorkflow:
        properties = {
            '@type': ['File', 'SoftwareSourceCode', 'ComputationalWorkflow', 'HowTo'],
            'name': file.name,
        }

        return self.crate.add_workflow(file, file.name, main=True, lang='cwl',
                                       lang_version='v1.0',
                                       properties=properties)

    def add_profile(self, id, name, version) -> ContextEntity:
        properties = {
            '@type': 'CreativeWork',
            'name': name,
            'version': version,
        }
        return self.crate.add(ContextEntity(self.crate, id, properties=properties))

    def add_tool(self, id, name, description) -> ContextEntity:
        properties = {
            '@type': 'SoftwareApplication',
            # Could also be ['SoftwareSourceCode', 'ComputationalWorflow', 'HowTo'] if it has steps
            'description': description,
            'name': name,
        }
        return self.crate.add(ContextEntity(self.crate, id, properties=properties))

    def add_software(self, id, name):
        properties = {'@type': 'SoftwareApplication', 'name': name}

        return self.crate.add(ContextEntity(self.crate, id, properties=properties))

    def add_step(self, id, position):
        properties = {
            '@type': 'HowToStep',
            'position': position,
        }
        return self.crate.add(ContextEntity(self.crate, id, properties=properties))

    def add_parameter(self, id, name=None, properties=None) -> ContextEntity:
        if name:
            properties = properties or {}
            properties['name'] = name

        return self.crate.add(ContextEntity(self.crate, id, properties=properties))

    def add_file(self, path: str):
        self.crate.add_file(path)

    def add_howto(self, path: str):
        self.crate.add_howto(path)

    def write(self):
        self.crate.write(self.path)
