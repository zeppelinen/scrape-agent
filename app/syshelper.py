import shlex
import subprocess
import sys

def run_cmd(cmd, callback=None, watch=False, shell=False):
    """Runs the given command and gathers the output.
    If a callback is provided, then the output is sent to it, otherwise it
    is just returned.
    Optionally, the output of the command can be "watched" and whenever new
    output is detected, it will be sent to the given `callback`.
    Args:
        cmd (str): The command to run.
    Kwargs:
        callback (func):  The callback to send the output to.
        watch (bool):     Whether to watch the output of the command.  If True,
                          then `callback` is required.
    Returns:
        A string containing the output of the command, or None if a `callback`
        was given.
    Raises:
        RuntimeError: When `watch` is True, but no callback is given.
    """

    if watch and not callback:
        raise RuntimeError('You must provide a callback when watching a process.')

    output = None
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=shell)

        if watch:
            while proc.poll() is None:
                line = proc.stdout.readline()
                if line != "":
                    callback(line)

            # Sometimes the process exits before we have all of the output, so
            # we need to gather the remainder of the output.
            remainder = proc.communicate()[0]
            if remainder:
                callback(remainder)
        else:
            output = proc.communicate()[0]
    except:
        err = str(sys.exc_info()[1]) + "\n"
        output = err

    if callback and output is not None:
        callback(output)
        return None

    return output