import os

import click
import pathlib

from parser.crate import get_crates


@click.group()
def cli():
    pass


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
