import enum

prospective_types = [
    ('File', 'SoftwareSourceCode', 'ComputationalWorkflow', 'HowTo'),
    'HowToStep',
    'SoftwareApplication',
    'FormalParameter',
]

retrospective_types = [
    'OrganizeAction',
    'ControlAction',
    'CreateAction',
    {'Person', 'Organisation'},
    {'File', 'PropertyValue'},
]


def _compare_types(item, types):
    if isinstance(types, tuple) and isinstance(item, (tuple, list)):
        return set(item) == set(types)

    if isinstance(types, set):
        return item in types

    if isinstance(types, str):
        return item == types

    return False


class CrateParts(str, enum.Enum):
    prospective = 'prospective'
    retrospective = 'retrospective'
    orchestration = 'orchestration'  # not sure if this is a thing
    metadata = 'metadata'
    other = 'other'  # mostly context items


def detect_crate_type(item: dict) -> CrateParts:
    """Check if item part of the prospective, retrospective, or metadata of a provenance crate"""
    if item['@id'] == 'ro-crate-metadata.json':
        return CrateParts.metadata

    if item['@id'] == './' or item['@type'] == 'Dataset':
        return CrateParts.orchestration

    for _type in prospective_types:
        if _compare_types(item['@type'], _type):
            return CrateParts.prospective

    for _type in retrospective_types:
        if _compare_types(item['@type'], _type):
            return CrateParts.retrospective

    return CrateParts.other
