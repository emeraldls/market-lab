"""Export the compiled token bytecode embedded by the CLI; run after forge build."""
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[3]
compiled = json.loads((root / "contracts/pools/out/TestToken.sol/TestToken.json").read_text())
exported = json.dumps({
    "bytecode": compiled["bytecode"]["object"],
    "runtime": compiled["deployedBytecode"]["object"],
}, indent=2) + "\n"
destination = root / "src/providers/elysium/test_token.json"
if "--check" in sys.argv:
    if destination.read_text() != exported:
        raise SystemExit("Embedded test token is outdated; run script/export_test_token.py")
else:
    destination.write_text(exported)
