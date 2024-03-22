from pathlib import Path

from rocrate.model import ContextEntity
from rocrate.rocrate import ROCrate

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

    def add_workflow(self, file: Path):
        properties = {
            '@type': ['File', 'SoftwareSourceCode', 'ComputationalWorkflow', 'HowTo'],
            'name': file.name,
        }

        return self.crate.add_workflow(file, file.name, main=True, lang='cwl',
                                           lang_version='v1.0',
                                           properties=properties)

    def add_tool(self, id, name, description):
        properties = {
            '@type': 'SoftwareApplication',
            # Could also be ['SoftwareSourceCode', 'ComputationalWorflow', 'HowTo'] if it has steps
            'description': description,
            'name': name,
        }
        return self.crate.add(ContextEntity(self.crate, id, properties=properties))

    def add_step(self, id, position):
        properties = {
            '@type': 'HoToStep',
            'position': position,
        }
        return self.crate.add(ContextEntity(self.crate, id, properties=properties))

    def add_parameter(self, id) -> ContextEntity:
        props = {
        }
        return self.crate.add(ContextEntity(self.crate, id, properties=props))

    def add_file(self, path: str):
        self.crate.add_file(path)

    def add_software(self, name: str, version: str, path: str):
        self.crate.add_software(name, version, path)

    def add_howto(self, path: str):
        self.crate.add_howto(path)

    def write(self):
        self.crate.write(self.path)