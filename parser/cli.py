import os

import click
import pathlib
from rocrate.rocrate import ROCrate


@click.group()
def cli():
    pass


@cli.command()
@click.argument('path', type=click.Path(exists=True, file_okay=False, path_type=pathlib.Path))
def list_subcrates(path):
    # Find all ro-crate-metadata.json (by name)
    for root, dir, files in os.walk(path):
        if 'ro-crate-metadata.json' in files:
            print(pathlib.Path(root) / 'ro-crate-metadata.json')
