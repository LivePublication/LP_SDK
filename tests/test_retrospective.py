import json
import os
import shutil
import tempfile
from pathlib import Path

from rocrate.model import ContextEntity

from lp_sdk.parser.retrospective import format_retro_rocrate, write_retro_rocrate
from lp_sdk.retrospective.crate import DistStepCrate
from lp_sdk.validation.util import CrateParts
from lp_sdk.validation.validator import Comparator


def test_create_retro_crate_manual():
    """TDD: manual creation of the retrospective parts of the crate"""
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)

        # Create crate
        crate = DistStepCrate(d)

        # Add files
        files = []
        for _id, examples in zip(*[['327fc7aedf4f6b69a42a7c8b808dc5a7aff61376', 'b9214658cc453331b62c2282b772a5c063dbd284',
                       '97fe1b50b4582cebc7d853796ebd62e3e163aa3f'],
            [
                ['packed.cwl#main/input', 'packed.cwl#revtool.cwl/input'],
                ["packed.cwl#main/output", "packed.cwl#sorttool.cwl/output"],
                ["packed.cwl#revtool.cwl/output", "packed.cwl#sorttool.cwl/input"]
            ]
        ]):
            shutil.copy(Path(__file__).parent / 'data' / 'cwl_prov' / f'{_id}', d)
            props = {
                'exampleOfWork': [{'@id': e} for e in examples]
            }
            files.append(crate.crate.add_file(d / _id, properties=props))

        property_values = [
            crate.crate.add(ContextEntity(
                crate.crate, _id,
                {
                    '@type': 'PropertyValue',
                    'name': name,
                    'value': value,
                    'exampleOfWork': {'@id': example},
                }
            ))
            for _id, name, value, example in zip(*[
                ['#pv-main/reverse_sort', '#pv-main/sorted/reverse'],
                ['main/reverse_sort', 'main/sorted/reverse'],
                ["True", "True"],
                ['packed.cwl#main/reverse_sort', 'packed.cwl#sorttool.cwl/reverse']
            ])
        ]

        # Add create action
        create_actions = [
            crate.crate.add(ContextEntity(
                crate.crate, _id,
                {
                    '@type': 'CreateAction',
                    'name': name,
                    'startTime': start_time,
                    'endTime': end_time,
                    'object': [{'@id': o.id} for o in objects],
                    'result': [{'@id': o.id} for o in results],
                    'instrument': {'@id': instrument},
                }
            ))
            for _id, start_time, end_time, name, objects, results, instrument in zip(*[
                ['#4154dad3-00cc-4e35-bb8f-a2de5cd7dc49', '#6933cce1-f8f0-4032-8848-e0fc9166e92f',
                 '#9eac64b2-c2c8-401f-9af8-7cfb0e998107'],
                ['2018-10-25T15:46:35.211153', '2018-10-25T15:46:35.314101', '2018-10-25T15:46:36.975235'],
                ['2018-10-25T15:46:43.020168', '2018-10-25T15:46:36.967359', '2018-10-25T15:46:38.069110'],
                ['Run of workflow/packed.cwl#main', 'Run of workflow/packed.cwl#main/rev',
                 'Run of workflow/packed.cwl#main/sorted'],
                [[files[0], property_values[0]], [files[0]], [files[2], property_values[1]]],
                [[files[1]], [files[2]], [files[1]]],
                ['packed.cwl', 'packed.cwl#revtool.cwl', 'packed.cwl#sorttool.cwl']
            ])
        ]

        # Add control actions
        cont_1 = crate.crate.add(ContextEntity(
            crate.crate, '#793b3df4-cbb7-4d17-94d4-0edb18566ed3',
            {
                '@type': 'ControlAction',
                'name': 'orchestrate sorttool.cwl',
                'object': {'@id': create_actions[2].id},
                'instrument': {'@id': 'packed.cwl#main/sorted'}
            }
        ))

        cont_2 = crate.crate.add(ContextEntity(
            crate.crate, '#4f7f887f-1b9b-4417-9beb-58618a125cc5',
            {
                '@type': 'ControlAction',
                'name': 'orchestrate revtool.cwl',
                'object': {'@id': create_actions[1].id},
                'instrument': {'@id': 'packed.cwl#main/rev'}
            }
        ))


        # Add organize action
        agent = crate.crate.add(ContextEntity(
            crate.crate, 'https://orcid.org/0000-0001-9842-9718',
            {
                '@type': 'Person',
                'name': 'Stian Soiland-Reyes'
            }))

        crate.crate.add(ContextEntity(
            crate.crate, '#d6ab3175-88f5-4b6a-b028-1b13e6d1a158',
            {
                '@type': 'OrganizeAction',
                'name': 'Run of cwltool 1.0.20181012180214',
                'startTime': '2018-10-25T15:46:35.210973',
                'agent': {'@id': agent.id},
                'object': [{'@id': cont_1.id}, {'@id': cont_2.id}],
                'result': {'@id': create_actions[0].id},
                'instrument': {'@id': '#a73fd902-8d14-48c9-835b-a5ba2f9149fd'}  # CWL software
            }
        ))

        crate.write()

        with open(d / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

    with open(Path(__file__).parent / 'data' / 'cwl_prov' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    comp = Comparator([CrateParts.retrospective], [CrateParts.prospective, CrateParts.metadata, CrateParts.orchestration, CrateParts.other],
                      expected)
    comp.compare(actual)


def test_create_retro_crate():
    """TDD: create retrospective crate using tooling"""
    # Expected result
    with open(Path(__file__).parent / 'data' / 'cwl_prov' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    # Manually define the data - i.e.: the results of the run
    workflow_data = {
        '@id': '#4154dad3-00cc-4e35-bb8f-a2de5cd7dc49',
        'startTime': '2018-10-25T15:46:35.211153',
        'endTime': '2018-10-25T15:46:43.020168',
        'name': 'Run of workflow/packed.cwl#main',
        'inputs': [
            {'@id': '327fc7aedf4f6b69a42a7c8b808dc5a7aff61376'},
            {'@id': '#pv-main/reverse_sort', 'value': 'True', 'name': 'main/reverse_sort'}
        ],
        'outputs': [{'@id': 'b9214658cc453331b62c2282b772a5c063dbd284'}],
    }
    step_data = [
        {
            '@id': '#4f7f887f-1b9b-4417-9beb-58618a125cc5',
            'name': 'orchestrate revtool.cwl',
            'create': {
                '@id': '#6933cce1-f8f0-4032-8848-e0fc9166e92f',
                'startTime': '2018-10-25T15:46:35.314101',
                'endTime': '2018-10-25T15:46:36.967359',
                'name': 'Run of workflow/packed.cwl#main/rev',
                'inputs': [{'@id': '327fc7aedf4f6b69a42a7c8b808dc5a7aff61376'}],
                'outputs': [{'@id': '97fe1b50b4582cebc7d853796ebd62e3e163aa3f'}],
            }
        },
        {
            '@id': '#793b3df4-cbb7-4d17-94d4-0edb18566ed3',
            'name': 'orchestrate sorttool.cwl',
            'create': {
                '@id': '#9eac64b2-c2c8-401f-9af8-7cfb0e998107',
                'startTime': '2018-10-25T15:46:36.975235',
                'endTime': '2018-10-25T15:46:38.069110',
                'name': 'Run of workflow/packed.cwl#main/sorted',
                'inputs': [
                    {'@id': '97fe1b50b4582cebc7d853796ebd62e3e163aa3f'},
                    {'@id': '#pv-main/sorted/reverse', 'value': 'True', 'name': 'main/sorted/reverse'}
                ],
                'outputs': [{'@id': 'b9214658cc453331b62c2282b772a5c063dbd284'}],
            }
        }
    ]

    source_dir = Path(__file__).parent / 'data' / 'cwl_prov'
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)

        # Create crate
        crate = DistStepCrate(d)

        def _add_file_props(items):
            rval = []
            for item in items:
                _id = item['@id']
                if (source_dir / _id).exists():
                    shutil.copy(source_dir / _id, d)
                    rval.append(crate.add_file(d / _id))
                else:
                    rval.append(crate.add_property(_id, item['name'], item['value']))
            return rval

        def _add_create(data):
            inputs = _add_file_props(data['inputs'])
            outputs = _add_file_props(data['outputs'])

            return crate.add_create_action(data['@id'], {
                'startTime': data['startTime'],
                'endTime': data['endTime'],
                'name': data['name'],
                'object': [{'@id': i.id} for i in inputs],
                'result': [{'@id': o.id} for o in outputs],
            })

        # Step actions
        control_ents = []
        for step in step_data:
            create_ent = _add_create(step['create'])

            control_ents.append(crate.add_control_action(step['@id'], step['name'], create_ent))

        # Workflow action
        wf_create_ent = _add_create(workflow_data)

        agent = crate.add_agent('https://orcid.org/0000-0001-9842-9718', 'Stian Soiland-Reyes')
        org_ent = crate.add_organize_action('#d6ab3175-88f5-4b6a-b028-1b13e6d1a158', 'Run of cwltool 1.0.20181012180214',
                                            {'startTime': '2018-10-25T15:46:35.210973'},
                                            agent, control_ents, wf_create_ent)

        # First gen the distributed step crate - missing all links to prospective data
        crate.write()
        with open(d / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

        comp = Comparator([CrateParts.retrospective], [], expected)
        comp.compare(actual)
        os.remove(d / 'ro-crate-metadata.json')

        # Next (separate test?) link back to prospective data, using cwl (or if possible, only prospective) as reference
        # TODO

        crate.write()

        with open(d / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

        comp = Comparator([CrateParts.retrospective],
                          [CrateParts.prospective, CrateParts.metadata, CrateParts.orchestration, CrateParts.other],
                          expected)
        comp.compare(actual)


def test_retrospective():
    # This is more documentation that test, for now
    # Get data from the action provider on execution statues, e.g.:
    execution_data = {
        'runtime': 120,
        'platform': 'Ubuntu 20.04',
        'cpu': 'Intel(R) Xeon(R) CPU @ 2.30GHz',
        'memory': '8GB',
        'disk': '100GB',
    }

    crate = format_retro_rocrate(execution_data)
    # Write the crate to a file
    with tempfile.TemporaryDirectory() as d:
        out_file = Path(d) / 'ro_crate_metadata.json'
        assert not out_file.exists()

        write_retro_rocrate(crate, out_file)
        assert out_file.exists()

        with open(out_file) as f:
            result = f.read()

    # TODO: check contents
    print(result)


if __name__ == '__main__':
    test_retrospective()