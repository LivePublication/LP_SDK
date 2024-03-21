from pathlib import Path

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

        # Conforms to provenance crate
        profiles = [
            {'@id': 'https://w3id.org/ro/crate/1.1'},
            {'@id': 'https://w3id.org/workflowhub/workflow-ro-crate/1.0'}
        ]
        self.crate.metadata['conformsTo'] = profiles

        # Root dataset conforms to provenance crate
        profiles = [
            {"@id": "https://w3id.org/ro/wfrun/process/0.1"},
            {"@id": "https://w3id.org/ro/wfrun/workflow/0.1"},
            {"@id": "https://w3id.org/ro/wfrun/provenance/0.1"},
            {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"}
        ]
        self.crate.root_dataset['conformsTo'] = profiles

    def add_file(self, path: str):
        self.crate.add_file(path)

    def add_software(self, name: str, version: str, path: str):
        self.crate.add_software(name, version, path)

    def add_workflow(self, path: str):
        self.crate.add_workflow(path)

    def add_howto(self, path: str):
        self.crate.add_howto(path)

    def write(self):
        self.crate.write(self.path)