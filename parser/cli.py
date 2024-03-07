from pathlib import Path

import click
import pathlib

from parser.crate import get_crates
from parser import prospective as _prospective


@click.group()
def cli():
    pass


@cli.command()
@click.option('-i', '--input', 'input_path',
              type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
              help='Input WEP')
@click.option('-o', '--output', 'output_path',
              type=click.Path(exists=False, file_okay=True, dir_okay=False, path_type=Path),
              help='Output ROCrate')
def prospective(input_path, output_path):
    """
    Very basic example of crate generation from WEP - nothing here is correct, everything will change
    :param input_path:
    :param output_path:
    :return:
    """
    wep_json = _prospective.load_wep(input_path)
    rocrate_json = _prospective.parse_wep_to_rocrate(wep_json)
    assert _prospective.validate_rocrate(rocrate_json)
    _prospective.write_rocrate(rocrate_json, output_path)


@cli.command()
@click.argument('path', type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path))
def list_subcrates(path):
    # Find all ro-crate-metadata.json (by name)
    for crate in get_crates(path):
        print(crate.name)


@cli.command()
@click.argument('path', type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path))
def list_outputs(path):
    for crate in get_crates(path):
        print(crate.name)
        for file in crate.get_by_type('File'):
            if file.id.startswith('output/'):
                print(f'  {file.id}')
