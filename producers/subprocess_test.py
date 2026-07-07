import subprocess
import sys

result = subprocess.run([sys.executable, "-c", "print('worker alive')"], capture_output=True, text=True, timeout=10)
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)
print("Return code:", result.returncode)