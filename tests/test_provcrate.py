import json
import tempfile
from pathlib import Path

from provenance.crate import LpProvCrate
from validation.util import detect_crate_type, CrateParts


def _throw_or_print(msg, error=True, indent=0):
    if error:
        raise AssertionError(msg)
    else:
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


def _compare_dicts(expected, actual, name='root', error=True, indent=0, graph=None):
    if graph is None:
        graph = {item['@id']: item for item in expected['@graph']}

    for key in expected:
        if _exclude_item(expected[key], graph):
            print(' ' * indent + key + '- skipped, not prospective')
            continue
        else:
            print(' ' * indent + key)

        if key not in actual:
            if _is_graph_like(expected[key]) and len(_filter_list(expected[key], graph)) == 0:
                # All items in list are retrospective
                continue
            _throw_or_print(f"Key {key} not in {name}: {actual}", error, indent + 2)
            continue

        if isinstance(expected[key], dict):
            _compare_dicts(expected[key], actual[key], key, error, indent + 2, graph=graph)
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
                _compare_dicts(d1_items, d2_items, key, error, indent+2, graph=graph)
            else:
                if expected[key] != actual[key]:
                    _throw_or_print(f"{expected[key]} != {actual[key]}", error, indent + 2)
        else:
            if expected[key] != actual[key]:
                _throw_or_print(f"{expected[key]} != {actual[key]}", error, indent)


def test_create_prov_crate():
    """
    Testing/TDD of tooling to recreate the example provenance crate from https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate
    The code here will be very manual, used to understand the structure and logic needed in the provcrate implementation.
    """

    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        with open(d / 'packed.cwl', 'w+') as f:
            f.write('dummy data')

        crate = LpProvCrate(d)
        # Add conforms to statements
        profiles = [
            {'@id': 'https://w3id.org/ro/crate/1.1'},
            {'@id': 'https://w3id.org/workflowhub/workflow-ro-crate/1.0'}
        ]
        crate.crate.metadata['conformsTo'] = profiles

        # Root dataset conforms to provenance crate
        profiles = [
            ("https://w3id.org/ro/wfrun/process/", "0.1", 'Process Run Crate'),
            ("https://w3id.org/ro/wfrun/workflow/", "0.1", 'Workflow Run Crate'),
            ("https://w3id.org/ro/wfrun/provenance/", "0.1", 'Provenance Run Crate'),
            ("https://w3id.org/workflowhub/workflow-ro-crate/", "1.0", 'Workflow RO-Crate'),
        ]
        profile_entities = [crate.add_profile(f'{p[0]}{p[1]}', p[2], p[1]) for p in profiles]
        crate.crate.root_dataset['conformsTo'] = profile_entities

        # Add workflow file
        wf = crate.add_workflow(d / 'packed.cwl')

        # Add tools
        rev_tool = crate.add_tool('packed.cwl#revtool.cwl', 'revtool.cwl', 'Reverse each line using the `rev` command')
        sort_tool = crate.add_tool('packed.cwl#sorttool.cwl', 'sorttool.cwl', 'Sort lines using the `sort` command')

        wf['hasPart'] = [rev_tool, sort_tool]

        rev_step = crate.add_step('packed.cwl#main/rev', "0")
        rev_step['workExample'] = rev_tool

        sort_step = crate.add_step('packed.cwl#main/sorted', "1")
        sort_step['workExample'] = sort_tool

        wf['step'] = [rev_step, sort_step]

        # Add formal parameters
        p1 = crate.add_parameter('packed.cwl#main/input')
        p1.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'defaultValue': 'file:///home/stain/src/cwltool/tests/wf/hello.txt',
                'encodingFormat': 'https://www.iana.org/assignments/media-types/text/plain',
                'name': 'main/input'
             }
        )

        p2 = crate.add_parameter('packed.cwl#main/reverse_sort')
        p2.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'Boolean',
                'defaultValue': 'True',
                'name': 'main/reverse_sort'
            }
        )
        wf['input'] = [p1, p2]

        p = crate.add_parameter('packed.cwl#main/output')
        p.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'main/output'
            }
        )
        wf['output'] = [p]

        p = crate.add_parameter('packed.cwl#revtool.cwl/input')
        p.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'revtool.cwl/input'
            }
        )
        rev_tool['input'] = [p]

        p = crate.add_parameter('packed.cwl#revtool.cwl/output')
        p.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'revtool.cwl/output'
            }
        )
        rev_tool['output'] = [p]

        p1 = crate.add_parameter('packed.cwl#sorttool.cwl/reverse')
        p1.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'Boolean',
                'name': 'sorttool.cwl/reverse'
            }
        )

        p2 = crate.add_parameter('packed.cwl#sorttool.cwl/input')
        p2.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'sorttool.cwl/input'
            }
        )
        sort_tool['input'] = [p1, p2]

        p = crate.add_parameter('packed.cwl#sorttool.cwl/output')
        p.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'sorttool.cwl/output'
            }
        )
        sort_tool['output'] = [p]

        crate.add_software('#a73fd902-8d14-48c9-835b-a5ba2f9149fd', 'cwltool 1.0.20181012180214')

        # TODO: better validation tools - exclude retrospective entities from validation in lists

        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            data = json.load(f)

    # Expected data
    with open(Path(__file__).parent / 'data' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    print()
    _compare_dicts(expected, data, error=False)


if __name__ == '__main__':
    test_create_prov_crate()