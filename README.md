# LP_SDK [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18255587.svg)](https://doi.org/10.5281/zenodo.18255587)


## What this repository does

LP_SDK is a Python software development kit that provides early validation and helper utilities for LivePublication RO-Crate profiles. It supports building, parsing, and validating provenance crates, including Distributed Step Crates (retrospective step execution) and provenance run crates derived from CWL or Globus Workflow Execution Plan (WEP) definitions. The SDK is explicitly **validation and developer tooling** rather than a full provenance collection system. The SDK targets LivePublication RO-Crate profile instances aligned with Workflow Run Crate / Provenance Run Crate patterns.

## Repository structure

- `lp_sdk/` — core SDK modules for validation, parsing, prospective and retrospective crate construction, and Gladier/Globus helpers.
- `tests/` — unit and integration tests, including example data under `tests/data/`.
- `docs/` — handover notes and development context.
- `pyproject.toml` / `requirements.txt` — dependencies and CLI entrypoint (`lp-sdk`).

## Inputs

- RO-Crate metadata JSON (`ro-crate-metadata.json`) for validation and comparison utilities.
- Workflow definitions:
  - CWL files for `LpProvCrate.build_from_cwl()` (prospective provenance).
  - Globus Flow WEP JSON for `LpProvCrate.build_from_wep()` and the CLI `lp-sdk prospective`.
- Directories containing RO-Crate metadata for `lp-sdk list-subcrates` and `lp-sdk list-outputs`.
- Integration tests expect Globus endpoint IDs and authenticated access (see `tests/integration/provenance_test.py`).

## Outputs

- Generated RO-Crate metadata JSON for prospective provenance (`ro-crate-metadata.json`).
- Distributed Step Crates representing retrospective step execution (`lp_sdk/retrospective/`).

## Quickstart
```bash
git clone https://github.com/LivePublication/LP_SDK.git
cd LP_SDK
pip install -e .
```

Minimal CLI smoke check (uses bundled test data):
```bash
lp-sdk list-subcrates tests/data
lp-sdk list-outputs tests/data
```

Experimental prospective crate generation:
```bash
# TODO: confirm current WEP input expectations for prospective generation
lp-sdk prospective -i path/to/WEP.json -o ro-crate-metadata.json
```

## How to cite
- GitHub: https://github.com/LivePublication/LP_SDK
- Zenodo DOI: (minted after release)

## License
Apache-2.0. See `LICENSE`.
