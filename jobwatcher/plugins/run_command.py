import shlex
import subprocess as sub
import os

def run_command(command):
    _command = shlex.split(command)
    try:
        DEV_NULL = open(os.devnull, "rb")
        process = sub.Popen(_command, env=dict(os.environ), stdout=sub.PIPE, stderr=sub.STDOUT, stdin=DEV_NULL)
        _output = process.communicate()[0]
        exitcode = process.poll()
        if exitcode != 0:
            log.error("Failed to run %s:\n%s" % (_command, _output))
        return _output
    except sub.CalledProcessError:
        log.error("Failed to run %s\n" % _command)
        exit(1)
    finally:
        DEV_NULL.close()
