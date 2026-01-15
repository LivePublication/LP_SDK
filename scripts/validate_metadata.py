import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = "1.0.0"


def load_json(path: Path):
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path.name}: {exc}") from exc


def load_cff(path: Path):
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:
        data = {}
        for line in path.read_text().splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            if line.startswith(" "):
                continue
            if ":" in line:
                key, value = line.split(":", 1)
                data[key.strip()] = value.strip().strip('"')
        return data
    return yaml.safe_load(path.read_text())


def check_zenodo() -> list[str]:
    errors = []
    path = ROOT / ".zenodo.json"
    data = load_json(path)

    for field in ("title", "description", "version", "license", "creators"):
        if field not in data:
            errors.append(f".zenodo.json missing '{field}'")

    if data.get("version") != VERSION:
        errors.append(".zenodo.json version must be 1.0.0")

    if data.get("license") != "apache-2.0":
        errors.append(".zenodo.json license must be 'apache-2.0'")

    creators = data.get("creators", [])
    for creator in creators:
        orcid = creator.get("orcid")
        if not orcid:
            errors.append(".zenodo.json creator missing ORCID")
            continue
        if orcid.startswith("http"):
            errors.append(".zenodo.json ORCID must be bare (no URL)")
        if not re.match(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$", orcid):
            errors.append(".zenodo.json ORCID format is invalid")

    return errors


def check_codemeta() -> list[str]:
    errors = []
    path = ROOT / "codemeta.json"
    data = load_json(path)

    if data.get("version") != VERSION:
        errors.append("codemeta.json version must be 1.0.0")

    return errors


def check_citation() -> list[str]:
    errors = []
    path = ROOT / "CITATION.cff"
    data = load_cff(path)

    if not isinstance(data, dict):
        errors.append("CITATION.cff did not parse into a dictionary")
        return errors

    if str(data.get("version")) != VERSION:
        errors.append("CITATION.cff version must be 1.0.0")

    return errors


def check_rocrate() -> list[str]:
    errors = []
    path = ROOT / "ro-crate-metadata.json"
    data = load_json(path)

    graph = data.get("@graph", [])
    ids = []
    for entity in graph:
        if isinstance(entity, dict) and "@id" in entity:
            ids.append(entity["@id"])

    seen = set()
    duplicates = set()
    for identifier in ids:
        if identifier in seen:
            duplicates.add(identifier)
        seen.add(identifier)

    if duplicates:
        errors.append(f"ro-crate-metadata.json has duplicate @id values: {sorted(duplicates)}")

    root = next((item for item in graph if item.get("@id") == "./"), None)
    if not root:
        errors.append("ro-crate-metadata.json missing root dataset './'")
        return errors

    has_part = root.get("hasPart", [])
    if isinstance(has_part, dict):
        has_part = [has_part]

    for part in has_part:
        part_id = part.get("@id") if isinstance(part, dict) else part
        if not part_id:
            continue
        if re.match(r"^[a-z]+://", str(part_id)):
            errors.append(f"hasPart entry should be relative, found URL: {part_id}")
            continue
        if str(part_id).startswith("#"):
            continue
        rel = str(part_id)
        if rel.startswith("./"):
            rel = rel[2:]
        if Path(rel).is_absolute():
            errors.append(f"hasPart entry should be relative, found absolute path: {part_id}")
            continue
        if not (ROOT / rel).exists():
            errors.append(f"hasPart entry does not exist: {part_id}")

    return errors


def main() -> int:
    errors = []
    errors.extend(check_zenodo())
    errors.extend(check_codemeta())
    errors.extend(check_citation())
    errors.extend(check_rocrate())

    if errors:
        print("Metadata validation failed:\n")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Metadata validation passed.\n")
    print("Human checklist:")
    print("- Confirm Quickstart commands run (or keep TODOs if not validated)")
    print("- Confirm LICENSE matches Apache-2.0 intent")
    print("- Confirm Zenodo GitHub integration is enabled")
    print("- Create GitHub release/tag 1.0.0")
    print("- Verify Zenodo DOI minted after release")
    print("- Patch metadata/docs with DOI in a 1.0.1 release")
    return 0


if __name__ == "__main__":
    sys.exit(main())
