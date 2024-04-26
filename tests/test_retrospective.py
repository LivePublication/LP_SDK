import json
import shutil
import tempfile
from pathlib import Path

from rocrate.model import ContextEntity

from lp_sdk.parser.retrospective import format_retro_rocrate, write_retro_rocrate
from lp_sdk.retrospective.crate import DistStepCrate
from lp_sdk.validation.util import CrateParts
from lp_sdk.validation.validator import Comparator


def test_create_retro_crate():
    """TDD: manual creation of the retrospective parts of the crate"""
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        out_file = d / 'ro-crate-metadata.json'

        # Create crate
        crate = DistStepCrate(d)

        # Add files
        files = []
        for _id in ['327fc7aedf4f6b69a42a7c8b808dc5a7aff61376', 'b9214658cc453331b62c2282b772a5c063dbd284',
                       '97fe1b50b4582cebc7d853796ebd62e3e163aa3f']:
            shutil.copy(Path(__file__).parent / 'data' / 'cwl_prov' / f'{_id}', d)
            files.append(crate.crate.add_file(d / _id))

        property_values = [
            crate.crate.add(ContextEntity(
                crate.crate, _id,
                {
                    '@type': 'PropertyValue',
                    'name': name,
                    'value': value,
                }
            ))
            for _id, name, value in zip(*[
                ['#pv-main/reverse_sort', '#pv-main/sorted/reverse'],
                ['main/reverse_sort', 'main/sorted/reverse'],
                ["True", "True"]])
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
                }
            ))
            for _id, start_time, end_time, name, objects, results in zip(*[
                ['#4154dad3-00cc-4e35-bb8f-a2de5cd7dc49', '#6933cce1-f8f0-4032-8848-e0fc9166e92f',
                 '#9eac64b2-c2c8-401f-9af8-7cfb0e998107'],
                ['2018-10-25T15:46:35.211153', '2018-10-25T15:46:35.314101', '2018-10-25T15:46:36.975235'],
                ['2018-10-25T15:46:43.020168', '2018-10-25T15:46:36.967359', '2018-10-25T15:46:38.069110'],
                ['Run of workflow/packed.cwl#main', 'Run of workflow/packed.cwl#main/rev',
                 'Run of workflow/packed.cwl#main/sorted'],
                [[files[0], property_values[0]], [files[0]], [files[2], property_values[1]]],
                [[files[1]], [files[2]], [files[1]]],
            ])
        ]

        # Add control actions
        cont_1 = crate.crate.add(ContextEntity(
            crate.crate, '#793b3df4-cbb7-4d17-94d4-0edb18566ed3',
            {
                '@type': 'ControlAction',
                'name': 'orchestrate sorttool.cwl',
                'object': {'@id': create_actions[2].id}
            }
        ))

        cont_2 = crate.crate.add(ContextEntity(
            crate.crate, '#4f7f887f-1b9b-4417-9beb-58618a125cc5',
            {
                '@type': 'ControlAction',
                'name': 'orchestrate revtool.cwl',
                'object': {'@id': create_actions[1].id}
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
                'result': {'@id': create_actions[0].id}
            }
        ))

        crate.write()

        assert out_file.exists()
        with open(d / 'ro-crate-metadata.json') as f:
            actual = json.load(f)

    with open(Path(__file__).parent / 'data' / 'cwl_prov' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    comp = Comparator([CrateParts.retrospective], [CrateParts.prospective, CrateParts.metadata, CrateParts.orchestration, CrateParts.other],
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