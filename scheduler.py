import json

from subprocess import Popen, PIPE
from threading import Thread
from time import sleep

import db


class CommandRunner(Thread):

    def __init__(self, cmd):
        Thread.__init__(self)
        self.cmd = cmd

    def run(self):
        process = Popen(self.cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

        while process.poll() is None:
            sleep(0.1)

        print('process')
        print(process.stdout.read().decode())
        print(process.stderr.read().decode())
        print(process.returncode)


def register(path):

    cmd = [
        'python',
        'process_runner.py',
        '--register',
        '-e', path,
    ]

    t = CommandRunner(cmd)
    t.start()


def schedule(session, path, payload=None):

    cmd = [
        'python',
        'process_runner.py',
        '--run',
        '-e', path,
    ]

    if payload:
        cmd += ['--json', json.dumps(payload)]

    t = CommandRunner(cmd)
    t.start()


if __name__ == '__main__':

    register('test_job.py')

    while True:
        session = db.Session()
        next_job_run = db.JobRun.next_scheduled(session, state='pending')
        if next_job_run:
            payload = {
                'job_run_id': next_job_run.job_run_id,
            }
            path = next_job_run.get_job(session).path
            schedule(session, path, payload)
        db.JobRun.clear_pending(session)
        next_task_run = db.TaskRun.next_scheduled(session, state='pending')
        if next_task_run:
            print('schedule Task')
            payload = {
                'task_run_id': next_task_run.task_run_id,
            }
            path = next_task_run.get_job(session).path
            schedule(session, path, payload)
        db.TaskRun.clear_pending(session)
        session.close()
        sleep(1)
