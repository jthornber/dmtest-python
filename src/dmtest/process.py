import subprocess


def run(command, raise_on_fail=True):
    proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, universal_newlines=True,
                            check=True)
    stdout, stderr = proc.communicate()
    return_code = proc.returncode
    return (return_code, stdout.strip(), stderr.strip())
