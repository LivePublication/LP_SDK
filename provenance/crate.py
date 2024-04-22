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

        self.add_workflow(wep_file)

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
