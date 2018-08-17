import shlex
import subprocess as sub
import os
import logging

log = logging.getLogger(__name__)

def run_command(command, env):
    _command = shlex.split(command)
    try:
        DEV_NULL = open(os.devnull, "rb")
        env.update(os.environ.copy())
        process = sub.Popen(_command, env=env, stdout=sub.PIPE, stderr=sub.STDOUT, stdin=DEV_NULL)
        _output = process.communicate()[0]
        return _output
    except sub.CalledProcessError:
        log.error("Failed to run %s\n" % _command)
        exit(1)
    finally:
        DEV_NULL.close()
