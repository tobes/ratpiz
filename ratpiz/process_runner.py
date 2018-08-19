import json
import argparse
import os
import sys

from ratpiz import Job
from ratpiz import db
from ratpiz import scheduler


parser = argparse.ArgumentParser(description='job processor')
parser.add_argument(
    '--manifest',
    '-m',
    dest='manifest',
    help='manifest path'
)
parser.add_argument(
    '--execute',
    '-e',
    dest='execution_path',
    help='execution path'
)
parser.add_argument(
    '--json',
    '-j',
    dest='json_payload',
    help='json payload'
)


def import_path(path):
    """
    import and return a module from a path
    """

    # python 2 only
    import imp
    module = imp.load_source('module_name', path)
    return module


def jobs_from_path(path, job_name=None):
    """
    return jobs from a path
    """

    job_module = import_path(path)

    jobs = []
    for name in dir(job_module):
        item = getattr(job_module, name)
        if isinstance(item, Job):
            if job_name and item.name != job_name:
                continue
            jobs.append(item)
    return jobs


def register():
    """
    register a job with the scheduler
    """
    path = sys.argv[1]
    cwd = os.getcwd()
    path = os.path.expanduser(os.path.join(cwd, path))
    print(cwd)
    print('register %s' % path)
    # for testing we register a job
    payload = {
        'action': 'register',
    }
    scheduler.run_command(path, payload=payload)


def main():
    print('main')
    # We have been called let us do what needs to be done.

    args = parser.parse_args()
    print(args)

    if args.json_payload:
        payload = json.loads(args.json_payload)
    else:
        payload = {}

    if payload.get('action') == 'register':
        jobs = jobs_from_path(args.execution_path)
        print('@@@@ job')
        for job in jobs:
            job.set_path(os.path.abspath(args.execution_path))
            session = db.Session()
            job.register(session)
            session.close()

    if payload.get('action') == 'run':
        session = db.Session()
        job_run_id = payload.get('job_run_id')
        if job_run_id:
            job_run = db.JobRun.get_by_id(session, job_run_id)
            job = job_run.get_job(session)
            job_obj = jobs_from_path(job.path, job.name)
            job_obj[0].run(session, job_run)

        task_run_id = payload.get('task_run_id')
        if task_run_id:
            task_run = db.TaskRun.get_by_id(session, task_run_id)
            job = task_run.get_job(session)
            job_obj = jobs_from_path(job.path, job.name)
            task_obj = job_obj[0].tasks.get(task_run.task_name)
            task_obj.run(session, task_run)
        session.close()


if __name__ == '__main__':
    main()
