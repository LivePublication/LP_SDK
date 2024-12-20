# Handover notes (20-12-2024)
## Current status
- tests/integration/provenance_test.py contains the most up to date, working implementation of a globus flow, including dist crate generatation and transfer
- tests/test_formal_parameters.py contains a working implementation of formal parameter identification and transfer automation. It has not been tested as a live flow. Expected issues:
	- auto-created transfers localise their files to the tool/function (i.e.: "ToolA/FuncA/input.txt" instead of "input.txt") in order to avoid collisions. The actual function doesn't currently know about this, so you'll need to provide an explicit path in input. This can be potentially be fixed by modifying how each function interprets its input (either a wrapper function that unpacks inputs, or flow definition mutation). See ProvenanceTool.localise_path()
	- Gladier uses classes (not objects) to define each state. That means that auto generated transfers are dynamically created classes (see line 203 of provenance_client.py). This causes issues in tests if the same client class is instantiated multiple times, but I don't know if it will cause issues in normal usage.
- tests/test_provcrate.py contains mostly working implementation of generating prospective crats from CWL/WEP. These aim to match the example crate from ROCrate, which leads to some very awkward decisions for the WEP crate.
- tests/test_globus_prospective.py contains a better implementation for globus, but still relies on a hacky method for resolving formal parameters, based on filenames repeated in transfers and function parameters. The test currently fails because the validator logic used can't validate a prospective-only crate - use a comparator instead.
- tests/test_retrospective.py:test_create_retro_crate() is a partial implementation of linking a dist step crate back to a provenance crate. Note that this is CWL, not globus/WEP.
- lp_sdk/validation contains a crate validator and comparator. The validator is incomplete, but the comparator provides very granular validation of a test crate against an expected one, including the option to validate only prospective/retrospective/other parts of the crate, and separately whether to validate links to parts that may not exist (e.g.: links to prospective in a dist step crate)
- the rocrate-validator python package is useful for validating complete crates, but not granular enough for validating known partial crates (prospective/retrospective only).

## Next steps
- update/replicate provenance_test.py utilising the functionality in test_formal_parameters.py
	- provide storage endpoint ids to each Tool and Client
	- localise the input/output file paths given to each function in input
	- remove all mention of transfers to/from_compute and RT_ST
- update test_globus_prospective.py to use a comparator rather than validator, which will allow more granularity of TDD, but requires an expected crate to compare to.
- re-implement test_globus_prospective.py:parser() to use the formal parameters now genrated, rather than hacky filename matching, in order to identify parameters and connection
- complete implementation of test_globus_prospective.py
- modify/complete test_retrospective.py:test_create_retro_crate() to work with WEP/prospective from the above.
- extend provenance_test.py example to generate a prospective crate pre-flow, gather the dist step crates post-flow, and combine them all
	- will require matching of task/function/state names, parameters/formal parameters/connections, file names between pro/retrospective

## Other notes
- Investigate w3id.org for hosting extensions to the ROCrate standard
- the provenance crate standard now includes a way of storing runtime/hardware requirements - see the bottom few sections of https://www.researchobject.org/workflow-run-crate/profiles/provenance_run_crate
- https://www.researchobject.org/workflow-run-crate/requirements is a maybe useful reference of which runtime stats are worth collecting
- Investigate using Guest Collections to restrict the auth scope needed for flow transfers, and avoid having to 'resume flows'. See live-publication project ticket.