import json
import argparse
import os
import sys

from ratpiz import db
from ratpiz import Job
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
        for job in jobs:
            job.set_path(os.path.abspath(args.execution_path))
            session = db.Session()
            job.register(session)
            session.close()
    if payload.get('action') == 'run':
        uuid = payload.get('uuid')
        if not uuid:
            print('No uuid')
        print('Running Event %s' % uuid)
        event_type = payload.get('event_type')

        session = db.Session()
        event_run = db.Event.get_by_uuid(session, uuid)
        event_run.set_state(session, db.RUNNING)
        print(event_run)
        if event_type == 'job':
            job_run = db.JobRun.get_by_uuid(session, uuid)
            print(job_run)
            job = job_run.get_job(session)
            print(job)
            job_obj = jobs_from_path(job.path, job.name)
            print(job_obj[0])
            save_state = job_obj[0].run(session, job_run)

        elif event_type == 'task':
            task_run = db.TaskRun.get_by_uuid(session, uuid)
            job = task_run.get_job(session)
            job_obj = jobs_from_path(job.path, job.name)
            task_obj = job_obj[0].tasks.get(task_run.task_name)
            save_state = task_obj.run(session, task_run)
        if save_state:
            print('%s %s' % (uuid, save_state))
            event_run.set_state(session, save_state)
        session.close()


if __name__ == '__main__':
    main()
