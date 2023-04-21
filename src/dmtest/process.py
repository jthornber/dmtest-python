import logging as log
import subprocess
import dmtest.dependency_tracker as dep


def run(command, raise_on_fail=True):
    log.info(f"running: '{command}'")

    # Register the exe with the dependency tracker
    exe = command.lstrip().split()[0]
    dep.add_exe(exe)

    proc = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    stdout, stderr = proc.communicate()
    if stdout:
        log.info(f"stdout:\n{stdout.rstrip()}")
    if stderr:
        log.info(f"stderr:\n{stderr.rstrip()}")
    if proc.returncode:
        log.info(f"return code: {proc.returncode}")
    return_code = proc.returncode
    if return_code and raise_on_fail:
        log.error("process failed unexpectedly, raising exception")
        raise subprocess.CalledProcessError(return_code, command)
    return (return_code, stdout.strip(), stderr.strip())
