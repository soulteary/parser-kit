Go report policy
================

Go Test Report v1.1.2 and Go Report Card v1.1.3 are pinned to formal release commits. Ordinary CI tests generate read-only evidence without a second test run. Existing native platforms, lint/dependency gates and release verification remain enabled. Coverage has no new minimum threshold.

Tags (including slashes), published releases and manual runs produce stable and raw artifacts. Only successful reports for the unchanged default-branch SHA may write the five report files. PRs never write reports. Artifact names come from the producer attempt, and download uses the public API with read-only actions access. Expired/missing/failed/in-progress evidence never suppresses fresh production; per-source writeback serializes.

Manual runs accept source_ref; leave it empty for the selected default branch. Published token releases explicitly dispatch reports for their tag.
