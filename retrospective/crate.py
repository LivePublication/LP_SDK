from pathlib import Path

from rocrate.model import ContextEntity
from rocrate.rocrate import ROCrate


class DistStepCrate:
    def __init__(self, path: str):
        self.path = Path(path)
        self.crate = ROCrate()

        self.build()

    def build(self):
        entity = self.crate.add(
            ContextEntity(
                self.crate, 'distributed_step',
                {
                    '@type': ['CreateAction', 'HowTo', 'ActionAccessSpecification', 'Schedule']
                }))

        self.crate.mainEntity = entity

    def add_position(self, task_id):
        position = self.crate.add(
            ContextEntity(
                self.crate,
                {
                    '@type': 'HowToStep',
                    'position': task_id
                }))

        self.crate.mainEntity['step'] = position

    def write(self):
        self.crate.write(self.path)
