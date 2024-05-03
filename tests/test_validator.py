import json
from pathlib import Path

import pytest

from lp_sdk.validation.schemas import provenance_crate_draft_schema
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
    validator = Validator(provenance_crate_draft_schema)

    # TODO: validate that the required items are present, i.e.:
    #  from https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate
    #  CreateAction + instrument for each tool
    #  ComputationalWorkflow MUST also be File, SoftwareSourceCode. Also HowTo if it contains steps
    #  Dataset.conformsTo must reference prov/process/run crate + rocrate
    #  tools must be references by workflow, files/parameters by actions, formalparameters by files, etc.

    validator.validate(data)