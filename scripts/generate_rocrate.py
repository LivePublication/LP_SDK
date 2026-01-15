import datetime
from pathlib import Path

from rocrate.rocrate import ROCrate  # type: ignore[import-not-found]
from rocrate.model.contextentity import ContextEntity  # type: ignore[import-not-found]
from rocrate.model.person import Person  # type: ignore[import-not-found]

TITLE = "LP_SDK: LivePublication SDK for RO-Crate Provenance Validation"
DESCRIPTION = (
    "A LivePublication SDK providing early validation tooling and helper utilities for "
    "RO-Crate provenance profiles (Workflow Run Crate / Provenance Run Crate patterns), "
    "including prospective and retrospective crate handling for distributed workflows."
)
VERSION = "1.0.1"
LICENSE_URL = "https://spdx.org/licenses/Apache-2.0.html"
REPO_URL = "https://github.com/LivePublication/LP_SDK"


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    crate = ROCrate()
    crate.root_dataset["name"] = TITLE
    crate.root_dataset["description"] = DESCRIPTION
    crate.root_dataset["datePublished"] = datetime.date.today().isoformat()
    crate.root_dataset["license"] = LICENSE_URL
    crate.root_dataset["version"] = VERSION

    author = crate.add(
        Person(
            crate,
            "https://orcid.org/0000-0001-8260-231X",
            {"name": "Augustus Ellerm"},
        )
    )

    software = crate.add(
        ContextEntity(
            crate,
            "#lp-sdk",
            {
                "@type": "SoftwareSourceCode",
                "name": TITLE,
                "description": DESCRIPTION,
                "version": VERSION,
                "license": LICENSE_URL,
                "codeRepository": REPO_URL,
                "programmingLanguage": "Python",
                "author": author,
            },
        )
    )
    crate.root_dataset["mainEntity"] = software
    crate.root_dataset["creator"] = [{"@id": author.id}]

    paths_to_add = [
        "README.md",
        "LICENSE",
        "CITATION.cff",
        ".zenodo.json",
        "codemeta.json",
        "pyproject.toml",
        "requirements.txt",
        "docs",
        "lp_sdk",
        "tests",
        "scripts",
    ]

    added_paths = set()

    for relative_path in paths_to_add:
        path = repo_root / relative_path
        if not path.exists():
            continue

        rel_path = path.relative_to(repo_root).as_posix()
        if path.is_dir():
            rel_path = rel_path.rstrip("/") + "/"

        if rel_path in added_paths:
            continue

        added_paths.add(rel_path)

        if path.is_dir():
            crate.add_directory(rel_path)
        else:
            crate.add_file(rel_path)

    crate.write(repo_root)


if __name__ == "__main__":
    main()
