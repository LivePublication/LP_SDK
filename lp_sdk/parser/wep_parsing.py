from typing import Any

from pydantic import BaseModel


def _chain_get(data: dict, key: str):
    """Get a value from a nested dictionary using a dot-separated key"""
    keys = key.strip('$.').split('.')
    for k in keys:
        data = data[k]
    return data


class InputValue(BaseModel):
    """Represents a globus lookup key and the value retrieved from input"""
    key: str
    value: Any


def _match_form(data: dict, input_: dict, key: str) -> str | InputValue:
    """Retrieve the value of a key from the WEP, handling literal keys, lookup keys (.$), and expression keys (.=)"""
    if key in data:
        return data[key]
    elif f'{key}.$' in data:
        return InputValue(key=data[f'{key}.$'], value=_chain_get(input_, data[f'{key}.$']))
    elif f'{key}.=' in data:
        raise NotImplementedError()
    raise KeyError()


class Task(BaseModel):
    """Information about a task to be executed in a Globus flow"""
    endpoint: str | InputValue
    function: str | InputValue
    payload: str | InputValue

    @staticmethod
    def parse(task: dict, input_: dict):
        return Task(
            endpoint=_match_form(task, input_, 'endpoint'),
            function=_match_form(task, input_, 'function'),
            payload=_match_form(task, input_, 'payload'),
        )


class TransferItem(BaseModel):
    source_path: str | InputValue
    destination_path: str | InputValue
    recursive: bool | InputValue

    @staticmethod
    def parse(transfer_item: dict, input_: dict):
        return TransferItem(
            source_path=_match_form(transfer_item, input_, 'source_path'),
            destination_path=_match_form(transfer_item, input_, 'destination_path'),
            recursive=_match_form(transfer_item, input_, 'recursive'),
        )


class ComputeState(BaseModel):
    name: str
    comment: str | None
    tasks: list[Task]
    resultPath: str
    position: int | None

    @staticmethod
    def parse(name: str, compute_state: dict, input_: dict, position: int = None):
        return ComputeState(
            name=name,
            comment=compute_state['Comment'],
            tasks=[Task.parse(task, input_) for task in compute_state['Parameters']['tasks']],
            resultPath=compute_state['ResultPath'],
            position=position,
        )


class TransferState(BaseModel):
    source_endpoint: str | InputValue
    destination_endpoint: str | InputValue
    transfer_items: list[TransferItem]

    @staticmethod
    def parse(transfer_state: dict, input_: dict):
        return TransferState(
            source_endpoint=_match_form(transfer_state['Parameters'], input_, 'source_endpoint'),
            destination_endpoint=_match_form(transfer_state['Parameters'], input_, 'destination_endpoint'),
            transfer_items=[TransferItem.parse(item, input_) for item in transfer_state['Parameters']['transfer_items']],
        )


def parse_states(wep: dict, input_: dict) -> tuple[list[ComputeState], list[TransferState]]:
    """Parse a WEP into a list of TransferState and ComputeState objects"""
    compute_states = []
    transfer_states = []
    pos = 0
    state_name = wep['StartAt']
    while True:
        state = wep['States'][state_name]
        if state['ActionUrl'] == 'https://transfer.actions.globus.org/transfer/':
            if 'provenance' in state_name:
                # TODO: better way of detecting these
                pass
            else:
                transfer_states.append(TransferState.parse(state, input_))
        elif state['ActionUrl'] == 'https://compute.actions.globus.org':
            compute_states.append(ComputeState.parse(state_name, state, input_, pos))
            pos += 1
        else:
            raise NotImplementedError(f'Unknown state type: {state["ActionUrl"]}')
        state_name = state.get('Next', None)
        if state_name is None:
            break

    return compute_states, transfer_states
