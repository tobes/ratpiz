# import platform
# import random
# from datetime import datetime
from datetime import tzinfo, timedelta, datetime

from croniter import croniter

import db

ZERO = timedelta(0)
UNIX_EPOC = datetime(1970, 1, 1)


def to_unix_time(datetime_):
    # python 3.3+ return datetime_.timestamp()
    return (datetime_ - UNIX_EPOC).total_seconds()


class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


utc = UTC()


class Fail(Exception):
    pass


class Retry(Exception):
    pass


class Context:

    def fail_task(self, message):
        raise Fail(message)

    def retry_task(self, message):
        raise Retry(message)


class Task:

    def __init__(self, action=None, **kwargs):
        # print(platform.python_version())
        # check for valid action ie callable
        self._action = action
        self._initial_kwargs = kwargs
        self.name = kwargs.get('name', action.__name__)
        self.status = 'waiting'
        self._upstream = []
        self._downstream = []

    def __repr__(self):
        return '<Task `%s` %s>' % (self.name, self.status)

    def run(self, session, task_run, context=None):
        print(
            'task %s running for %s ...' %
            (task_run.task_name, task_run.due_time)
        )
        if task_run.state == 'pending':
            task_run.set_state(session, 'running')
        elif task_run.state == 'retry':
            task_run.set_state(session, 'running')

        if context is None:
            context = Context()

        result = None
        exception = None
        try:
            result = self._action(context)
            state = 'success'
        except Fail as e:
            state = 'fail'
        except Exception as e:
            exception = e
            state = 'retry'

        if exception:
            print(str(exception))

        if state == 'retry':
            max_retries = self.from_kwargs('max_retries', 2)
            if task_run.retries >= max_retries:
                print('maximum number of retries')
                state = 'fail'

        if state == 'retry':
            retry_delay = self.from_kwargs('retry_delay', 1)
            task_run.set_retry(session, retry_delay)

        else:
            task_run.complete(session, state=state)
            job_run = db.JobRun.get_by_id(session, task_run.job_run_id)
            job_run.set_state(session, 'waiting')
        print('status %s' % self.status)
        print('result %s' % result)

    def from_kwargs(self, key, default=None):
        return self._initial_kwargs.get(key, default)


class Job:

    def __init__(self, name, **kwargs):
        self.name = name
        self._initial_kwargs = kwargs
        self.tasks = {}
        self.dependencies = {}
        self.completed_tasks = set()
        self.path = None

        start_date = kwargs.get('start_date')
        # TODO parse start date from string?
        self.start_date = start_date
        self.schedule = kwargs.get('schedule')

    def set_path(self, path):
        self.path = path

    def __repr__(self):
        return '<Job `%s` %s tasks>' % (self.name, len(self.tasks))

    def add_task(self, task):
        assert task.name not in self.tasks, (
            'Task `%s` already added' % task.name
        )
        self.tasks[task.name] = task
        self.dependencies[task.name] = set()

    def set_dependency(self, task_1, task_2):
        self.dependencies[task_1.name].add(task_2.name)

    def show_dependencies(self):
        print(self.dependencies)

    def next_due_time(self):
        return datetime.now()

    def register(self, session):
        job_db = db.Job.register(session, self)
        self.set_schedule(session, job_db)

    def schedule_tasks(self, session, job_run):
        # add tasks as needed
        print('schedule_tasks')
        completed_tasks = set()
        for task_run in job_run.get_completed_tasks(session):
            completed_tasks.add(task_run.task_name)
        if completed_tasks == set(self.tasks.keys()):
            print('all tasks complete')
            self.run_completed(session, job_run)
            return
        print('completed_tasks %s' % completed_tasks)

        for task_name, dependencies in self.dependencies.items():
            if task_name in completed_tasks:
                print('task %s completed' % task_name)
                continue
            print('TASK: %s %s' % (task_name, dependencies))
            if dependencies - completed_tasks:
                print(
                    'task %s awaiting dependencies %s' %
                    (task_name, dependencies)
                )
                continue

            print('adding task %s' % task_name)
            task_run = db.TaskRun.add_run(
                    session,
                    job_run.due_time,
                    job_run_id=job_run.job_run_id,
                    task_name=task_name,
                    job_id=job_run.job_id,
            )

    def set_schedule(self, session, job_db):
        print('set schedule')

        if job_db.scheduled(session):
            print('no need to schedule')
            return
        base_date = job_db.last_run or self.start_date
        schedule = croniter(self.schedule, base_date)

        next_schedule = schedule.get_next()
        # check if we missed a run
        if job_db.last_run is None:
            prev_schedule = schedule.get_prev()
            if prev_schedule >= to_unix_time(base_date):
                next_schedule = prev_schedule
        next_schedule_dt = datetime.fromtimestamp(next_schedule, utc)

        print('schedule for %s' % datetime.fromtimestamp(next_schedule, utc))
        db.JobRun.add_run(session, next_schedule_dt, job_id=job_db.job_id)

    def run(self, session, job_run):
        print('job running for %s ...' % job_run.due_time)
        if job_run.state == 'pending':
            job_run.set_state(session, 'running')
            self.schedule_tasks(session, job_run)
        if job_run.set_state == 'running':
            print('...running...')

    def run_completed(self, session, job_run):
        job_run.complete(session)
        job_db = db.Job.get_by_id(session, job_run.job_id)
        self.set_schedule(session, job_db)
