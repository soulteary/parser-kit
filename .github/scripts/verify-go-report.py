import json
import subprocess
import sys

# The CLI can exit zero even when a checker could not execute.
result = subprocess.run(
    ["goreportcard-cli", "-d", sys.argv[1] if len(sys.argv) > 1 else ".", "-j"],
    check=True, capture_output=True, text=True,
)
report = json.loads(result.stdout)
required = {"gofmt", "go_vet", "ineffassign", "gocyclo", "license", "misspell"}
checks = report.get("checks", [])
if {check.get("name") for check in checks} != required:
    raise SystemExit("Go Report Card did not return the complete checker set")
errors = [check for check in checks if check.get("error")]
if report.get("did_error") or errors:
    for check in errors:
        print(check["name"] + ": " + check["error"], file=sys.stderr)
    raise SystemExit("Refusing to publish incomplete Go Report Card evidence")
