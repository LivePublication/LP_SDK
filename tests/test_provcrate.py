import json
import tempfile
from pathlib import Path

from provenance.crate import LpProvCrate
from tests.util import compare_dicts


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
        # This is done automatically
        # profiles = [
        #     {'@id': 'https://w3id.org/ro/crate/1.1'},
        #     {'@id': 'https://w3id.org/workflowhub/workflow-ro-crate/1.0'}
        # ]
        # crate.crate.metadata['conformsTo'] = profiles

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
    compare_dicts(expected, data, error=True)


if __name__ == '__main__':
    test_create_prov_crate()