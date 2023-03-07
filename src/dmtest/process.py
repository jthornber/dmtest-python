import subprocess


def run(command, raise_on_fail=True):
    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    stdout, stderr = proc.communicate()
    return_code = proc.returncode
    if return_code and raise_on_fail:
        print(f"stdout: {stdout}")
        print(f"stderr: {stderr}")
        raise subprocess.CalledProcessError(return_code, command)
    return (return_code, stdout.strip(), stderr.strip())
