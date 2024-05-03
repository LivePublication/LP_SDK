import json
from pathlib import Path

import pytest

from lp_sdk.validation.validator import Validator


def test_validator_is_id():
    assert Validator._is_id_or_list({'@id': 'id'}), 'Single id should pass'
    assert not Validator._is_id_or_list({'@id': 'id', 'other': 'stuff'}), 'Single id cannot contain other keys'
    assert not Validator._is_id_or_list({'@id': 5}), '@id must be string'
    assert not Validator._is_id_or_list({}), 'empty reference should fail'

    assert Validator._is_id_or_list([{'@id': 'id'}]), 'List of ids should pass'
    assert Validator._is_id_or_list([{'@id': 'id'}, {'@id': 'id2'}]), 'List of ids should pass'
    assert not Validator._is_id_or_list([{'@id': 'id'}, {'@id': 'id2'}, {}]), 'Empty reference in list'
    assert not Validator._is_id_or_list([{'@id': 'id', 'other': 'stuff'}]), 'List of ids cannot contain other keys'
    assert not Validator._is_id_or_list([{'@id': 5}]), '@id must be string within list'
    assert not Validator._is_id_or_list([{'@id': id}, 5]), 'List must contain only references'

    assert not Validator._is_id_or_list([[{'@id': 'id'}]]), 'Nested list of ids should fail'


def test_validator_get_reference_list():
    assert Validator._get_reference_list({'@id': 'id'}) == ['id'], 'Failed to retrieve single id'
    assert Validator._get_reference_list([{'@id': 'id'}]) == ['id'], 'Failed to retrieve list of ids'
    assert Validator._get_reference_list([{'@id': 'id'}, {'@id': 'id2'}]) == ['id', 'id2'], \
        'Failed to retrieve list of ids'

    with pytest.raises(AssertionError, match='not a valid reference'):
        Validator._get_reference_list({'@id': 'id', 'other': 'stuff'})



def test_validator():
    """TDD: test Validator against example provcrate, ensuring it handles all types present"""
    crate_path = Path(__file__).parent / 'data' / 'cwl_prov' / 'ro-crate-metadata.json'
    with open(crate_path) as f:
        data = json.load(f)

    # TODO - properly model type inheritance and marginality in schema
    validator = Validator({
        'Thing': { # Every type is also implicitly a thing
            'allowed': ['name', 'description', 'additionalType', 'alternateName', 'identifier', 'url'],
            'references': ['identifier', 'url']
        },
        'CreativeWork': {
            'allowed': ['about', 'conformsTo', 'version'],
            'references': ['about', 'conformsTo'],
        },
        'Dataset': {
            'required': ['conformsTo', 'hasPart', 'mainEntity', 'mentions'],
            'references': ['conformsTo', 'hasPart', 'mainEntity', 'mentions'],
        },
        'ComputationalWorkflow': {
            # Should, according to bioschemas, also require: conformsTo, creator, dateCreated, publisher, url, version
            'required': ['input', 'output', 'programmingLanguage'],
            'allowed': ['hasPart'],
            'references': ['input', 'output', 'programmingLanguage', 'hasPart'],
        },
        'File': {  # Not sure where this schema is defined
            'allowed': ['exampleOfWork'],
            'references': ['exampleOfWork']
        },
        'SoftwareSourceCode': {},
        'HowTo': {
            'required': ['step'],
            'references': ['step'],
        },
        'ComputerLanguage': {
            'allowed': ['version']  # from CreativeWork?
        },
        'FormalParameter': {
            'required': ['name'],  # Bioschemas requires conformsTo
            'allowed': ['encodingFormat', 'defaultValue']
        },
        'HowToStep': {
            'required': ['position', 'workExample'],
            'references': ['workExample']
        },
        'SoftwareApplication': {
            'allowed': ['input', 'output'],  # Not actually in schema?
            'references': ['input', 'output']
        },
        'Person': {},
        'OrganizeAction': {
            'required': ['agent', 'instrument', 'object', 'result', 'startTime'],
            'references': ['agent', 'instrument', 'object', 'result']
        },
        'CreateAction': {
            'required': ['startTime', 'endTime', 'instrument', 'object', 'result'],
            'allowed': ['environment'],  # TODO - not in example, but may be used for env variables
            'references': ['instrument', 'object', 'result']
        },
        'ControlAction': {
            'required': ['instrument', 'object'],
            'references': ['instrument', 'object']
        },
        'PropertyValue': {
            'required': ['value'],
            'allowed': ['exampleOfWork'],  # Not actually in schema?
            'references': ['exampleOfWork']
        }
    })

    # TODO: validate that the required items are present, i.e.:
    #  from https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate
    #  CreateAction + instrument for each tool
    #  ComputationalWorkflow MUST also be File, SoftwareSourceCode. Also HowTo if it contains steps
    #  Dataset.conformsTo must reference prov/process/run crate + rocrate
    #  tools must be references by workflow, files/parameters by actions, formalparameters by files, etc.

    validator.validate(data)