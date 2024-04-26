from pathlib import Path

from rocrate.model import ContextEntity
from rocrate.rocrate import ROCrate


class DistStepCrate:
    def __init__(self, path: str):
        self.path = Path(path)
        self.crate = ROCrate()

        self.build()
        self.files = {}

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
                crate=self.crate,
                properties={
                    '@type': 'HowToStep',
                    'position': task_id
                }))

        self.crate.mainEntity['step'] = position

    def add_organize_action(self, id: str, name: str, properties: dict, agent: ContextEntity,
                            step_actions: list[ContextEntity], wf_action: ContextEntity):
        return self.crate.add(ContextEntity(
            self.crate,
            identifier=id, properties={
                '@type': 'OrganizeAction',
                'name': name,
                'object': [{'@id': act.id} for act in step_actions],
                'result': {'@id': wf_action.id},
                'agent': {'@id': agent.id},
                **properties
            }
        ))

    def add_control_action(self, id: str, name: str, create_action: ContextEntity):
        return self.crate.add(ContextEntity(
            self.crate,
            identifier=id, properties={
                '@type': 'ControlAction',
                'name': name,
                'object': {'@id': create_action.id}
            }
        ))

    def add_create_action(self, id, properties):
        return self.crate.add(ContextEntity(
            self.crate,
            identifier=id, properties={
                '@type': 'CreateAction',
                **properties
            }
        ))

    def add_agent(self, id, name):
        return self.crate.add(ContextEntity(
            self.crate,
            identifier=id, properties={
                '@type': 'Person',
                'name': name
            }
        ))

    def add_property(self, id, name, value):
        return self.crate.add(ContextEntity(
            self.crate,
            identifier=id, properties={
                '@type': 'PropertyValue',
                'name': name,
                'value': value
            }
        ))

    def add_file(self, path: str):
        if path in self.files:
            return self.files[path]
        else:
            file = self.crate.add_file(self.path / path)
            self.files[path] = file
            return file

    def write(self):
        self.crate.write(self.path)
