import json

from subprocess import Popen, PIPE
from threading import Thread
from time import sleep

import db


class CommandRunner(Thread):
    """
    Run the command in a process so that it is isolated.
    Monitor the process and log output.
    """

    def __init__(self, cmd):
        Thread.__init__(self)
        self.cmd = cmd

    def run(self):
        """
        Run the command.
        """
        # run the command
        process = Popen(self.cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

        # monitor until it finishes
        while process.poll() is None:
            # sleep so as not to use too many resources
            sleep(1)

        # logging
        print('process')
        print(process.stdout.read().decode())
        print(process.stderr.read().decode())
        print(process.returncode)


def schedule(path, payload=None):

    cmd = [
        'python',
        'process_runner.py',
        '-e', path,
    ]

    if payload:
        cmd += ['--json', json.dumps(payload)]

    # run command in a thread
    t = CommandRunner(cmd)
    t.start()


if __name__ == '__main__':

    # for testing we register a job
    payload = {
        'action': 'register',
    }
    schedule('test_job.py', payload)

    session = db.Session()
    try:
        while True:
            # any jobs need to run?
            next_job_run = db.JobRun.next_scheduled(session, state='pending')
            if next_job_run:
                print('schedule Job')
                payload = {
                    'action': 'run',
                    'job_run_id': next_job_run.job_run_id,
                }
                path = next_job_run.get_job(session).path
                schedule(path, payload)

            # clear any jobs that are pending
            db.JobRun.clear_pending(session)

            # any tasks need to run?
            next_task_run = db.TaskRun.next_scheduled(session, state='pending')
            if next_task_run:
                print('schedule Task')
                payload = {
                    'action': 'run',
                    'task_run_id': next_task_run.task_run_id,
                }
                path = next_task_run.get_job(session).path
                schedule(path, payload)

            # clear any tasks that are pending
            db.TaskRun.clear_pending(session)

            # sleep so as not to use too many resources
            sleep(1)

    except KeyboardInterrupt:
        print('stopping...')
    session.close()
