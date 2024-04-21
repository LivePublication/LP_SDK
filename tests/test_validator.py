import json
import tempfile
from pathlib import Path

import pytest

from provenance.crate import LpProvCrate
from tests.util import compare_dicts
from validation.util import CrateParts
from validation.validator import Comparator


def _gen_commands():
    """Crate a list of lambdas, to be called in order, which should create a valid crate"""
    def dataset_conforms(crate: LpProvCrate, context: dict) -> dict:
        profiles = [
            ("https://w3id.org/ro/wfrun/process/", "0.1", 'Process Run Crate'),
            ("https://w3id.org/ro/wfrun/workflow/", "0.1", 'Workflow Run Crate'),
            ("https://w3id.org/ro/wfrun/provenance/", "0.1", 'Provenance Run Crate'),
            ("https://w3id.org/workflowhub/workflow-ro-crate/", "1.0", 'Workflow RO-Crate'),
        ]
        profile_entities = [crate.add_profile(f'{p[0]}{p[1]}', p[2], p[1]) for p in profiles]
        crate.crate.root_dataset['conformsTo'] = profile_entities

        return {}

    def wf_input_1(crate: LpProvCrate, context: dict) -> dict:
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
        return {'wf_input_1': p1}

    def wf_input_2(crate: LpProvCrate, context: dict) -> dict:
        p2 = crate.add_parameter('packed.cwl#main/reverse_sort')
        p2.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'Boolean',
                'defaultValue': 'True',
                'name': 'main/reverse_sort'
            }
        )
        return {'wf_input_2': p2}

    def wf_output(crate: LpProvCrate, context: dict) -> dict:
        p = crate.add_parameter('packed.cwl#main/output')
        p.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'main/output'
            }
        )
        return {'wf_output': p}

    def rev_input(crate: LpProvCrate, context: dict) -> dict:
        p = crate.add_parameter('packed.cwl#revtool.cwl/input')
        p.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'revtool.cwl/input'
            }
        )
        return {'rev_input': p}

    def rev_output(crate: LpProvCrate, context: dict) -> dict:
        p = crate.add_parameter('packed.cwl#revtool.cwl/output')
        p.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'revtool.cwl/output'
            }
        )
        return {'rev_output': p}

    def sort_input_1(crate: LpProvCrate, context: dict) -> dict:
        p1 = crate.add_parameter('packed.cwl#sorttool.cwl/reverse')
        p1.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'Boolean',
                'name': 'sorttool.cwl/reverse'
            }
        )
        return {'sort_input_1': p1}

    def sort_input_2(crate: LpProvCrate, context: dict) -> dict:
        p2 = crate.add_parameter('packed.cwl#sorttool.cwl/input')
        p2.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'sorttool.cwl/input'
            }
        )
        return {'sort_input_2': p2}

    def sort_output(crate: LpProvCrate, context: dict) -> dict:
        p = crate.add_parameter('packed.cwl#sorttool.cwl/output')
        p.properties().update(
            {
                '@type': 'FormalParameter',
                'additionalType': 'File',
                'name': 'sorttool.cwl/output'
            }
        )
        return {'sort_output': p}

    def rev_tool(crate: LpProvCrate, context: dict) -> dict:
        rev_tool = crate.add_tool('packed.cwl#revtool.cwl', 'revtool.cwl', 'Reverse each line using the `rev` command')
        if 'rev_input' in context:
            rev_tool['input'] = [context['rev_input']]
        if 'rev_output' in context:
            rev_tool['output'] = [context['rev_output']]
        return {'rev_tool': rev_tool}

    def sort_tool(crate: LpProvCrate, context: dict) -> dict:
        sort_tool = crate.add_tool('packed.cwl#sorttool.cwl', 'sorttool.cwl', 'Sort lines using the `sort` command')
        sort_tool['input'] = [context[k] for k in ['sort_input_1', 'sort_input_2'] if k in context]
        if 'sort_output' in context:
            sort_tool['output'] = [context['sort_output']]
        return {'sort_tool': sort_tool}

    def rev_step(crate: LpProvCrate, context: dict) -> dict:
        rev_step = crate.add_step('packed.cwl#main/rev', "0")
        if 'rev_tool' in context:
            rev_step['workExample'] = context['rev_tool']
        return {'rev_step': rev_step}

    def sort_step(crate: LpProvCrate, context: dict) -> dict:
        sort_step = crate.add_step('packed.cwl#main/sorted', "1")
        if 'sort_tool' in context:
            sort_step['workExample'] = context['sort_tool']
        return {'sort_step': sort_step}

    def add_workflow(crate: LpProvCrate, context: dict) -> dict:
        wf = crate.add_workflow(context['path'] / 'packed.cwl')

        wf['hasPart'] = [context[k] for k in ['rev_tool', 'sort_tool'] if k in context]
        wf['step'] = [context[k] for k in ['rev_step', 'sort_step'] if k in context]

        wf['input'] = [context[k] for k in ['wf_input_1', 'wf_input_2'] if k in context]
        if 'wf_output' in context:
            wf['output'] = [context['wf_output']]

        return {'wf': wf}

    def add_software(crate: LpProvCrate, context: dict) -> dict:
        software = crate.add_software('#a73fd902-8d14-48c9-835b-a5ba2f9149fd', 'cwltool 1.0.20181012180214')
        return {'software': software}

    return [
        dataset_conforms,
        wf_input_1,
        wf_input_2,
        wf_output,
        rev_input,
        rev_output,
        sort_input_1,
        sort_input_2,
        sort_output,
        rev_tool,
        sort_tool,
        rev_step,
        sort_step,
        add_workflow,
        add_software,
    ]


class _TestCommands(str):
    """Container class - used so that parametrized tests can be named sensibly"""
    __test__ = False

    def __new__(cls, name: str, funcs: list[callable]):
        obj = super().__new__(cls, name)
        obj.name = name
        obj.funcs = funcs
        return obj


def _gen_missing_commands():
    commands = _gen_commands()

    for j in range(len(commands)):
        par_name = commands[j].__name__
        yield _TestCommands(par_name, [c for i, c in enumerate(commands) if i != j])


def _apply_commands(commands) -> dict:
    """Builds a crate by executing a list of commands, returns resulting crate metadata"""
    with tempfile.TemporaryDirectory() as d:
        d = Path(d)
        with open(d / 'packed.cwl', 'w+') as f:
            f.write('dummy data')

        # Apply each command to crate
        context = {'path': d}
        crate = LpProvCrate(d)
        for c in commands:
            context.update(c(crate, context))

        crate.write()

        with open(Path(d) / 'ro-crate-metadata.json') as f:
            return json.load(f)


def test_crude_validator_succeeds():
    commands = _gen_commands()
    actual = _apply_commands(commands)

    with open(Path(__file__).parent / 'data' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    compare_dicts(expected, actual, error=True)


@pytest.mark.parametrize('commands', _gen_missing_commands())
def test_crude_validator_fails(commands: _TestCommands):
    actual = _apply_commands(commands.funcs)

    with open(Path(__file__).parent / 'data' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    with pytest.raises(AssertionError):
        compare_dicts(expected, actual, error=True)


def test_comparator_succeeds():
    commands = _gen_commands()
    actual = _apply_commands(commands)

    with open(Path(__file__).parent / 'data' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    comp = Comparator([CrateParts.prospective, CrateParts.metadata, CrateParts.orchestration, CrateParts.other], expected)
    assert comp.compare(actual), "Expected comparator to confirm partial match"


@pytest.mark.parametrize('commands', _gen_missing_commands())
def test_comparator_fails(commands: _TestCommands):
    actual = _apply_commands(commands.funcs)

    with open(Path(__file__).parent / 'data' / 'ro-crate-metadata.json') as f:
        expected = json.load(f)

    with pytest.raises(AssertionError):
        comp = Comparator([CrateParts.prospective, CrateParts.metadata, CrateParts.orchestration, CrateParts.other], expected)
        assert comp.compare(actual), "Expected comparator to confirm partial match"



