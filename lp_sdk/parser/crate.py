from rocrate.rocrate import ROCrate
import os
import pathlib


def get_crates(path: pathlib.Path):
    assert path.is_dir(), f"Expecting directory, got {path}"
    assert path.exists(), f"Directory does not exist: {path}"

    for root, dir, files in os.walk(path):
        if 'ro-crate-metadata.json' in files:
            yield ROCrate(str(pathlib.Path(root)))
