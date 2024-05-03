

class Validator:
    def __init__(self, schema):
        # TODO: handle multiple schemas (e.g.: provcrate + parameter connections + globus)
        self.schema = schema

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
