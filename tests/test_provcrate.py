import json
import shutil
import tempfile
from pathlib import Path

from provenance.crate import LpProvCrate
from validation.util import CrateParts
from validation.validator import Comparator


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

        # Add formal parameters
        wf_input_1 = crate.add_parameter('packed.cwl#main/input')
        wf_input_1.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'defaultValue': 'file:///home/stain/src/cwltool/tests/wf/hello.txt',
                'encodingFormat': 'https://www.iana.org/assignments/media-types/text/plain',
                'name': 'main/input'
             }
        )

        wf_input_2 = crate.add_parameter('packed.cwl#main/reverse_sort')
        wf_input_2.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'Boolean',
                'defaultValue': 'True',
                'name': 'main/reverse_sort'
            }
        )

        wf_output = crate.add_parameter('packed.cwl#main/output')
        wf_output.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'main/output'
            }
        )

        rev_input = crate.add_parameter('packed.cwl#revtool.cwl/input')
        rev_input.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'revtool.cwl/input'
            }
        )

        rev_output = crate.add_parameter('packed.cwl#revtool.cwl/output')
        rev_output.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'revtool.cwl/output'
            }
        )

        sort_input_1 = crate.add_parameter('packed.cwl#sorttool.cwl/reverse')
        sort_input_1.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'Boolean',
                'name': 'sorttool.cwl/reverse'
            }
        )

        sort_input_2 = crate.add_parameter('packed.cwl#sorttool.cwl/input')
        sort_input_2.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'sorttool.cwl/input'
            }
        )

        sort_output = crate.add_parameter('packed.cwl#sorttool.cwl/output')
        sort_output.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'sorttool.cwl/output'
            }
        )


        # Add tools
        rev_tool = crate.add_tool('packed.cwl#revtool.cwl', 'revtool.cwl', 'Reverse each line using the `rev` command')
        rev_tool['input'] = [rev_input]
        rev_tool['output'] = [rev_output]

        sort_tool = crate.add_tool('packed.cwl#sorttool.cwl', 'sorttool.cwl', 'Sort lines using the `sort` command')
        sort_tool['input'] = [sort_input_1, sort_input_2]
        sort_tool['output'] = [sort_output]

        # Add steps
        rev_step = crate.add_step('packed.cwl#main/rev', "0")
        rev_step['workExample'] = rev_tool

        sort_step = crate.add_step('packed.cwl#main/sorted', "1")
        sort_step['workExample'] = sort_tool

        # Add workflow file
        wf = crate.add_workflow(d / 'packed.cwl')
        wf['hasPart'] = [rev_tool, sort_tool]

        wf['step'] = [rev_step, sort_step]
        wf['input'] = [wf_input_1, wf_input_2]
        wf['output'] = [wf_output]

        crate.add_software('#a73fd902-8d14-48c9-835b-a5ba2f9149fd', 'cwltool 1.0.20181012180214')

        # TODO: better validation tools - exclude retrospective entities from validation in lists

        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

    # Expected data
    with open(Path(__file__).parent / 'data' / 'cwl_prov' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    comp = Comparator([CrateParts.prospective, CrateParts.metadata, CrateParts.other, CrateParts.orchestration],
                      expected)
    comp.compare(actual)


def test_create_prov_crate_from_cwl():
    """
    Testing/TDD of tooling to recreate the example provenance crate from https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate
    This code will recreate the above test using a more automated approach, from the CWL file.
    """
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)

        # Input CWL file
        input_cwl = Path(__file__).parent / 'data' / 'cwl_prov' / 'packed.cwl'
        input_cwl = Path(shutil.copy(input_cwl, d))

        # Build crate from CWL file
        crate = LpProvCrate(d)
        crate.build_from_wf(input_cwl)

        # TODO: this is considered prospective - but runcrate gets this by running the workflow
        # See also: https://github.com/common-workflow-language/cwlprov-py/blob/main/cwlprov/prov.py
        crate.add_software('#a73fd902-8d14-48c9-835b-a5ba2f9149fd', 'cwltool 1.0.20181012180214')

        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

    # Expected data
    # TODO: the unit tests in runcrate expect more than this, but also read more than just the .cwl file
    with open(Path(__file__).parent / 'data' / 'cwl_prov' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    comp = Comparator([CrateParts.prospective, CrateParts.metadata, CrateParts.other, CrateParts.orchestration],
                      expected)
    comp.compare(actual)


def test_prov_crate_from_wep():
    """
    Testing/TDD of tooling to recreate the example provenance crate from https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate
    This code will recreate the above test using a more automated approach, from the WEP file.
    """
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)

        # Input WEP file
        input_wep = Path(__file__).parent / 'data' / 'WEP.json'
        input_wep = Path(shutil.copy(input_wep, d))

        # Build crate from WEP file
        crate = LpProvCrate(d)
        crate.build_from_wep(input_wep)
        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

    # Expected data
    # TODO: the unit tests in runcrate expect more than this, but also read more than just the .cwl file
    with open(Path(__file__).parent / 'data' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    comp = Comparator([CrateParts.prospective, CrateParts.metadata, CrateParts.other, CrateParts.orchestration],
                      expected)
    comp.compare(actual)
