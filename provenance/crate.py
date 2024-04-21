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

    def build_from_wf(self, wf_file: Path):
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

        # Add workflow
        wf_defs = convert.get_workflow(wf_file)
        wf = self.add_workflow(wf_file)

        pos_map = convert.ProvCrateBuilder._get_step_maps(wf_defs)

        # Add steps
        for step in getattr(wf_defs[wf.id], 'steps', []):
            id = f'{step.id.split("#")[-1]}'
            pos = pos_map[wf.id][id]['pos']
            step_ent = self.add_step(f'{wf.id}#{id}', pos)
            wf.append_to('step', step_ent)

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

    def add_tool(self, id, name, description):
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

    def add_parameter(self, id) -> ContextEntity:
        props = {
        }
        return self.crate.add(ContextEntity(self.crate, id, properties=props))

    def add_file(self, path: str):
        self.crate.add_file(path)

    def add_howto(self, path: str):
        self.crate.add_howto(path)

    def write(self):
        self.crate.write(self.path)