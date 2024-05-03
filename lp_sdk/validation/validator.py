

class Validator:
    def __init__(self, schema):
        # TODO: handle multiple schemas (e.g.: provcrate + parameter connections + globus)
        self.schema = schema

    def validate(self, data: dict):
        """Checks that every item in the data conforms to the schema"""
        # TODO: also validate that a list of expected ids is present
        # TODO: also validate that items exist of expected types (to match provenance crate schema)
        # TODO: allow filtering of items to check by CrateParts type

        # Top level contains context and graph
        assert '@context' in data, "Data does not contain a context"
        assert isinstance(data['@context'], str), "Context is not a string"
        assert '@graph' in data, "Data does not contain a graph"
        assert isinstance(data['@graph'], list), "Graph is not a list"

        # TODO: check that conformsTo statements match the scheme being checked against
        #  i.e.: schema is complete, and not excessive

        # Validate each item in the graph
        graph_dict = {item['@id']: item for item in data['@graph']}
        for item in data['@graph']:
            self._validate_item(item, graph_dict)

    def _validate_item(self, item: dict, graph: dict):
        """Validate a single item in the graph"""
        # Each item should have an @id and @type
        assert isinstance(item, dict), f"Item {item} is not a dictionary"
        assert '@id' in item, f"Item {item} does not have an id"
        assert isinstance(item['@id'], str), f"Item {item['@id']} id is not a string"
        assert '@type' in item, f"Item {item['@id']} does not have a type"
        assert isinstance(item['@type'], (str, list)), f"Item @type: {item['@type']} should be str or list[str]"

        # Check that type is in the schema
        _type = [item['@type']] if isinstance(item['@type'], str) else item['@type']
        _type += ['Thing']  # Every type is also implicitly a thing
        # TODO: consider additionalTypes
        for t in _type:
            assert t in self.schema, f"Item {item['@id']} type {t} not in schema"

            # Check that all keys required by the schema are present
            for key in self.schema[t].get('required', []):
                if key not in item:
                    assert key in self.schema[t], f"Item {item['@id']} is missing key {key}, required for type {t}"

        # Check that all keys in the item are allowed by the schema
        allowed_keys = {'@id', '@type'}
        for t in _type:
            allowed_keys |= set(self.schema[t].get('required', []) + self.schema[t].get('allowed', []))

        for key in item:
            assert key in allowed_keys, f"Item {item['@id']} key {key} not allowed by schema"

        # All items should either be references (lists) or strings
        reference_items = set()
        for t in _type:
            reference_items |= set(self.schema[t].get('references', []))

        for key in item:
            if key in ['@id', '@type']:
                continue
            if key in reference_items:
                assert self._is_id_or_list(item[key]), f"Item {item['@id']}:{key} is not a valid reference or list of references"

                # Check that all references are in the graph
                # Exception: references to rocrate specifications
                if item['@id'] and key == 'conformsTo':
                    continue
                # TODO: check this - identifiers/urls may be external references
                if key in ('identifier', 'url'):
                    continue
                for ref in self._get_reference_list(item[key]):
                    assert ref in graph, f"Item {item['@id']}:{key} references {ref} not in graph"
            else:
                assert not self._is_id_or_list(item[key]), f"Item {item['@id']}:{key} is a reference, should be a string"
                assert isinstance(item[key], str), f"Item {item['@id']}:{key} is not a string"

    @staticmethod
    def _is_id_or_list(item, _accept_list=True) -> bool:
        """Check that an item is either single reference, or list of references"""
        if isinstance(item, dict):
            # Single reference - must be a dict containing @id and nothing else, id must be string
            return '@id' in item and isinstance(item['@id'], str) and len(item) == 1
        elif isinstance(item, list) and _accept_list:
            # List of references, each should be valid as above
            return all(Validator._is_id_or_list(i, _accept_list=False) for i in item)
        return False

    @staticmethod
    def _get_reference_list(item) -> list[str]:
        """Get list of ids from a reference or list of references"""
        assert Validator._is_id_or_list(item), f"Item {item} is not a valid reference or list of references"
        if isinstance(item, dict):
            return [item['@id']]
        elif isinstance(item, list):
            return [i['@id'] for i in item]
        return []
