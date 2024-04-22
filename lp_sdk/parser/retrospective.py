import json
import uuid
from pathlib import Path


def format_retro_rocrate(data: dict) -> dict:
    # Not sure where these come from, yet:
    basics = {
        '@context': 'https://w3id.org/ro/crate/1.1/context',
        '@graph': []
    }

    basics['@graph'].append({
        '@id': str(uuid.uuid4()),
        '@type': 'CreateAction',
        **data
                            })

    return basics


def write_retro_rocrate(data: dict, path: Path):
    assert not path.exists(), f"Output path {path} already exists"

    with open(path, 'w+') as f:
        json.dump(data, f, indent=2)
