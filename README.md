# LivePublication SDK (LP_SDK)

**A Python toolkit for building, validating, and parsing LivePublication RO-Crate profiles**

The LivePublication SDK provides developer tools for working with RO-Crate-based workflow provenance following the [Workflow Run Crate](https://www.researchobject.org/workflow-run-crate/) family of profiles.

---

## **Crate Validation**

Validate RO-Crate metadata against LivePublication profile requirements:

- **`lp_sdk/validation/validator.py`**: `Validator` class checks RO-Crate JSON-LD structure, entity types, and required properties.
- **`lp_sdk/validation/schemas.py`**: `provenance_crate_draft_schema` defines structural rules for Provenance Run Crate profiles.
- **`lp_sdk/validation/comparator.py`**: `Comparator` class enables fine-grained comparison of partial crates (e.g., validating only prospective provenance without retrospective execution data).
- **`lp_sdk/validation/util.py`**: Utilities for classifying crate entities into prospective, retrospective, orchestration, and metadata categories.

### **Crate Building**

Programmatically construct RO-Crates from workflow definitions:

- **`lp_sdk/provenance/crate.py`**: `LpProvCrate` class builds Provenance Run Crates from CWL (Common Workflow Language) or WEP (Workflow Execution Plan / Globus Flow) definitions
  - `build_from_cwl()`: Generate prospective provenance from CWL workflow files
  - `build_from_wep()`: Generate prospective provenance from Globus Flow definitions
  - Helper methods for adding workflows, tools, steps, formal parameters, and connections
- **`lp_sdk/retrospective/crate.py`**: `DistStepCrate` class creates "Distributed Step Crates" documenting individual workflow step executions with retrospective provenance (runtime data, files created, agents, environment)

### **Crate Parsing & Introspection**

Load and analyze existing RO-Crates:

- **`lp_sdk/parser/crate.py`**: `get_crates()` recursively discovers and loads RO-Crates from directory trees
- **`lp_sdk/parser/wep_parsing.py`**: Structured parsing of Globus Flow definitions into typed models (`ComputeState`, `TransferState`, `Task`, etc.)
- **CLI tools** (see below): Commands for listing subcrates and extracting outputs---

## Quickstart

### Installation

```bash
# From PyPI (TODO: confirm if published)
pip install LP-SDK

# Or from source
git clone https://github.com/LivePublication/LP_SDK.git
cd LP_SDK
pip install -e .

# For development (includes test dependencies)
pip install -e ".[test]"
```

**Requirements**: Python ≥3.10

### CLI Usage

The SDK provides a `lp-sdk` command-line tool:

```bash
# Convert WEP (Globus Flow) to prospective RO-Crate
lp-sdk prospective -i workflow.json -o ro-crate-metadata.json

# List all RO-Crates in a directory tree
lp-sdk list-subcrates /path/to/workflow/results/

# List output files from all crates
lp-sdk list-outputs /path/to/workflow/results/
```

**Note**: The `prospective` command is experimental and marked in code as subject to change.

### Programmatic Usage: Validating a Crate

```python
import json
from lp_sdk.validation.validator import Validator
from lp_sdk.validation.schemas import provenance_crate_draft_schema

# Load RO-Crate metadata
with open('ro-crate-metadata.json') as f:
    crate_data = json.load(f)

# Validate structure
validator = Validator(provenance_crate_draft_schema)
try:
    validator.validate(crate_data)
    print("✓ Crate is valid")
except AssertionError as e:
    print(f"✗ Validation failed: {e}")
```

### Programmatic Usage: Parsing Existing Crates

```python
from pathlib import Path
from lp_sdk.parser.crate import get_crates

# Find all crates in directory tree
results_dir = Path("/path/to/workflow/results")
for crate in get_crates(results_dir):
    print(f"Found crate: {crate.name}")
  
    # Access crate metadata
    print(f"  Main entity: {crate.mainEntity}")
  
    # List output files
    for file in crate.get_by_type('File'):
        if file.id.startswith('output/'):
            print(f"  Output: {file.id}")
```

## Development

### Running Tests

```bash
# Install dev dependencies
pip install -e ".[test]"

# Run all tests
pytest

# Run with coverage
pytest --cov=lp_sdk --cov-report=html

# Run specific test
pytest tests/test_validator.py -v
```

### Linting

```bash
# Using ruff (configured in pyproject.toml)
ruff check .
```

## Contributing

This SDK is under active development:

- Complete automated provenance collection for Globus workflows
- Implement publication crate assembly from prospective + retrospective crates
- Enhance WEP formal parameter extraction (use function signatures rather than filename heuristics)
- Complete validation logic (entity presence, type distribution, semantic checks)
- Add support for runtime/hardware requirement metadata

**Maintainers**:

- Cornelis Drost (nelis.drost@auckland.ac.nz)
- Augustus Ellerm (gus.ellerm@pg.canterbury.ac.nz)
