from validation.util import detect_crate_type, CrateParts


def _throw_or_print(msg, error=True, indent=0):
    if error:
        raise AssertionError(msg)
    else:
        print(' ' * indent + msg)


def _print_if_printing(msg, error=True, indent=0):
    if not error:
        print(' ' * indent + msg)


def _exclude_item(item, graph: dict = None) -> bool:
    # Item is in the graph, and is a retrospective type
    if '@type' in item and detect_crate_type(item) == CrateParts.retrospective:
        return True
    # Item is a link to the graph, and item in graph is retrospective
    if graph is not None:
        if '@id' in item and _exclude_item(graph.get(item['@id'], {})):
            return True
    return False


def _filter_list(item, graph: dict = None) -> dict:
    return {i['@id']: i for i in item if not _exclude_item(i, graph)}


def _is_graph_like(item) -> bool:
    return isinstance(item, list) and len(item) > 0 and isinstance(item[0], dict) and '@id' in item[0]


def compare_dicts(expected, actual, name='root', error=True, indent=0, graph=None):
    if graph is None:
        graph = {item['@id']: item for item in expected['@graph']}

    for key in expected:
        if _exclude_item(expected[key], graph):
            _print_if_printing(f'{key} - skipped, not prospective', error, indent)
            continue
        else:
            _print_if_printing(key, error, indent)

        if key not in actual:
            if _is_graph_like(expected[key]) and len(_filter_list(expected[key], graph)) == 0:
                # All items in list are retrospective
                continue
            _throw_or_print(f"Key {key} not in {name}: {actual}", error, indent + 2)
            continue

        if isinstance(expected[key], dict):
            compare_dicts(expected[key], actual[key], key, error, indent + 2, graph=graph)
        elif isinstance(expected[key], list):
            if isinstance(expected[key][0], dict):#key == '@graph':
                # Build dicts of items by id
                if not isinstance(actual[key], list):
                    _throw_or_print(f"Item {key}: {actual[key]} is not a list", error, indent + 2)
                    continue

                # if key == '@graph':
                #     d1 = {item['@id']: item for item in expected[key]}
                d1_items = {}
                for item in expected[key]:
                    if _exclude_item(item, graph):
                        # Don't check for items in lists that would not have been added at prospective time
                        # TODO - build a proper validator
                        continue
                    else:
                        d1_items[item['@id']] = item
                d2_items = {item['@id']: item for item in actual[key]}
                compare_dicts(d1_items, d2_items, key, error, indent + 2, graph=graph)
            else:
                if expected[key] != actual[key]:
                    _throw_or_print(f"{expected[key]} != {actual[key]}", error, indent + 2)
        else:
            if expected[key] != actual[key]:
                _throw_or_print(f"{expected[key]} != {actual[key]}", error, indent)
