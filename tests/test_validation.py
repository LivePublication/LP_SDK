from lp_sdk.validation.util import detect_crate_type, CrateParts


def test_type_detection():
    assert detect_crate_type({'@id': 'ro-crate-metadata.json'}) == CrateParts.metadata
    assert detect_crate_type({'@id': './'}) == CrateParts.orchestration

    assert detect_crate_type({
        '@id': 'id', '@type': ['File', 'SoftwareSourceCode', 'ComputationalWorkflow', 'HowTo']
    }) == CrateParts.prospective
    assert detect_crate_type({'@id': 'id', '@type': 'Dataset'}) == CrateParts.orchestration
    assert detect_crate_type({'@id': 'id', '@type': 'CreativeWork'}) == CrateParts.other
    assert detect_crate_type({'@id': 'id', '@type': 'ComputerLanguage'}) == CrateParts.other
    assert detect_crate_type({'@id': 'id', '@type': 'FormalParameter'}) == CrateParts.prospective
    assert detect_crate_type({'@id': 'id', '@type': 'HowToStep'}) == CrateParts.prospective
    assert detect_crate_type({'@id': 'id', '@type': 'SoftwareApplication'}) == CrateParts.prospective
    assert detect_crate_type({'@id': 'id', '@type': 'OrganizeAction'}) == CrateParts.retrospective
    assert detect_crate_type({'@id': 'id', '@type': 'Person'}) == CrateParts.retrospective
    assert detect_crate_type({'@id': 'id', '@type': 'CreateAction'}) == CrateParts.retrospective
    assert detect_crate_type({'@id': 'id', '@type': 'PropertyValue'}) == CrateParts.retrospective
    assert detect_crate_type({'@id': 'id', '@type': 'ControlAction'}) == CrateParts.retrospective
    assert detect_crate_type({'@id': 'id', '@type': 'File'}) == CrateParts.retrospective
