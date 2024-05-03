provenance_crate_draft_schema = {
    'Thing': {  # Every type is also implicitly a thing
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
}
