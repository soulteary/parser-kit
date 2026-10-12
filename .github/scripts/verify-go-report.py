import json
import pathlib
import subprocess
import sys
import tempfile

# Validate the exact invocation that creates the published files.
directory = sys.argv[1] if len(sys.argv) > 1 else "."
output = pathlib.Path(".github") if directory == "." else pathlib.Path(".github/reports") / directory
output.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory() as temporary:
    temporary = pathlib.Path(temporary)
    result = subprocess.run(
        ["goreportcard-cli", "-d", directory, "-j",
         "-svg", str(temporary / "goreportcard.svg"),
         "-report", str(temporary / "goreportcard-report.md")],
        check=True, capture_output=True, text=True,
    )
    # The CLI emits write confirmations before its final JSON line.
    report = json.loads(result.stdout.splitlines()[-1])
    required = {"gofmt", "go_vet", "ineffassign", "gocyclo", "license", "misspell"}
    checks = report.get("checks", [])
    if {check.get("name") for check in checks} != required:
        raise SystemExit("Go Report Card did not return the complete checker set")
    errors = [check for check in checks if check.get("error")]
    if report.get("did_error") or errors:
        for check in errors:
            print(check["name"] + ": " + check["error"], file=sys.stderr)
        raise SystemExit("Refusing to publish incomplete Go Report Card evidence")
    for filename in ("goreportcard.svg", "goreportcard-report.md"):
        target = output / filename
        if target.is_symlink():
            raise SystemExit("Refusing a symlink report target")
        target.write_bytes((temporary / filename).read_bytes())
