import os.path

from croniter import croniter

from ratpiz import db
from ratpiz import timezone
from ratpiz import exceptions

from ratpiz.context import Context

from ratpiz.constants import (
    RETRY,
    RUNNING,
    FAIL,
    WAITING,
    SUCCESS,
    PENDING,

    TYPE_JOB,
    TYPE_TASK,
)


DEFAULT_KWARGS = {
    'retries': 1,
    'retry_delay': 5,
}


class Task:
    """
    A task which is part of a job.

    When triggered the task will call the action with a Context.
    """

    def __init__(self, action=None, **kwargs):
        # print(platform.python_version())
        # check for valid action ie callable
        self._action = action
        self.name = kwargs.pop('name', action.__name__)
        # we store any extra keywords
        self._initial_kwargs = kwargs
        self.status = WAITING
        self.parent_job = None

    def __repr__(self):
        return '<Task `%s` %s>' % (self.name, self.status)

    def set_parent_job(self, parent_job):
        self.parent_job = parent_job

    def run_task(self, session, task_run, context=None):
        """
        Trigger the task.

        :param session: a sqlalchemy session
        :param task_run: sqlalchemy model
        :param context: context to pass to the task action
        """

        if context is None:
            context = Context(task_run=task_run, session=session)

        print(
            'task %s running for %s ...' %
            (task_run.task_name, task_run.due_time)
        )

        if task_run.state == PENDING:
            task_run.set_state(session, RUNNING)
        elif task_run.state == RETRY:
            task_run.set_state(session, RUNNING)

        # run the task action catching any exceptions
        result = None
        exception = None
        try:
            result = self._action(context)
            state = SUCCESS
        except exceptions.Fail as e:
            state = FAIL
        except exceptions.Retry as e:
            state = RETRY
        except Exception as e:  # should this be BaseException?
            exception = e
            state = RETRY

        # log the exception
        if exception:
            # TODO backtrace
            print(str(exception))

        # if retrying should we now fail?
        if state == RETRY:
            max_retries = self.from_kwargs('retries')
            if task_run.retries >= max_retries:
                print('maximum number of retries')
                state = FAIL

        # What's going on? update the task run
        if state == RETRY:
            # we want to retry
            retry_delay = self.from_kwargs('retry_delay')
            task_run.set_retry(session, retry_delay)
        else:
            # the task has completed.
            task_run.complete(session, state=state)
            job_run = db.JobRun.get_by_id(session, task_run.job_run_id)
            job_run.set_state(session, WAITING)
            event = db.Event.get_by_uuid(session, job_run.uuid)
            event.set_state(session, WAITING)

        # logging is good
        print('status %s' % self.status)
        print('result %s' % result)
        return state

    def from_kwargs(self, key, default=None):
        """
        Helper function to get a value from any extra keyword arguments
        """
        try:
            return self._initial_kwargs[key]
        except KeyError:
            pass
        # if we didn't have does the parent job?
        if self.parent_job:
            return self.parent_job.from_kwargs(key, default)
        return default or DEFAULT_KWARGS.get(key)


class Job:
    """
    A job which has a number of tasks to run.
    """

    def __init__(self, name, **kwargs):
        self.name = name
        # TODO parse start date from string?
        self.start_date = kwargs.pop('start_date', None)
        self.schedule = kwargs.pop('schedule', None)
        # python_path
        python_path = kwargs.pop('python_path', None)
        if python_path:
            python_path = os.path.expanduser(python_path)
        self.python_path = python_path
        # we store any extra keywords
        self._initial_kwargs = kwargs

        self.tasks = {}
        self.dependencies = {}
        # path where the job is defined
        self.path = None

    def __repr__(self):
        return '<Job `%s` %s tasks>' % (self.name, len(self.tasks))

    def set_path(self, path):
        """
        set the path of the python file where the job is defined
        """
        self.path = path

    def add_task(self, task):
        """
        Add a task to a job
        """
        assert task.name not in self.tasks, (
            'Task `%s` already added' % task.name
        )
        task.set_parent_job(self)
        self.tasks[task.name] = task
        self.dependencies[task.name] = set()

    def set_dependency(self, task_1, task_2):
        """
        set task_2 as a dependent of task_1
        """
        self.dependencies[task_1.name].add(task_2.name)

    def show_dependencies(self):
        """
        show the current dependencies as best as we can
        """
        print(self.dependencies)

    def register(self, session):
        """
        job should register itself
        """

        job_db = db.Job.register(session, self)
        self.set_schedule(session, job_db)

    def schedule_tasks(self, session, job_run):
        """
        See which tasks need to be scheduled and add them to the scheduled as
        needed.
        """

        print('schedule_tasks')

        # get the completed tasks
        completed_tasks = set()
        for task_run in job_run.get_completed_tasks(session):
            completed_tasks.add(task_run.task_name)
        if completed_tasks == set(self.tasks.keys()):
            # all our tasks are done :)
            print('all tasks complete')
            self.run_completed(session, job_run)
            return SUCCESS
        print('completed_tasks %s' % completed_tasks)

        # get and check each task for the job
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

            # add task
            print('adding task %s' % task_name)
            task = task_run = db.TaskRun.add_run(
                    session,
                    job_run.due_time,
                    job_run_id=job_run.job_run_id,
                    task_name=task_name,
                    job_id=job_run.job_id,
            )
            print('added %s' % task.uuid)
            db.Event.add_run(
                    session,
                    due_time=task.due_time,
                    event_type=TYPE_TASK,
                    uuid=task.uuid,
            )

    def set_schedule(self, session, job_db):
        """
        see if this job needs to be scheduled to run.  if so the we add
        ourself.
        """
        print('set schedule')

        # are we already scheduled?
        if job_db.scheduled(session):
            print('no need to schedule')
            return

        # when are we next due to run?
        base_date = job_db.last_run or self.start_date
        schedule = croniter(self.schedule, base_date)
        next_schedule = schedule.get_next()

        # check if we missed a run
        # if it is the first time we run, then is it that the start date met
        # the run criteria?
        if job_db.last_run is None:
            prev_schedule = schedule.get_prev()
            if prev_schedule >= timezone.to_unix_time(base_date):
                next_schedule = prev_schedule

        # get the date as utc
        next_schedule_dt = timezone.datetime_from_timestamp(next_schedule)
        print('schedule for %s' % next_schedule_dt)
        # add job to schedule so that we are run
        job = db.JobRun.add_run(
            session, next_schedule_dt, job_id=job_db.job_id
        )
        print('added %s' % job.uuid)
        db.Event.add_run(
                session,
                due_time=job.due_time,
                event_type=TYPE_JOB,
                uuid=job.uuid,
        )

    def run_job(self, session, job_run):
        """
        Run the job.
        Check which tasks if any need to be scheduled and do any bookkeeping
        related to the job.
        """
        print('job running for %s ...' % job_run.due_time)
        job_run.set_state(session, RUNNING)
        state = self.schedule_tasks(session, job_run)
        return state

    def run_completed(self, session, job_run):
        """
        All tasks have been processed.  Do any required bookkeeping.
        """
        # mark job as completed in database
        job_run.complete(session)
        # set the schedule for our next update
        job_db = db.Job.get_by_id(session, job_run.job_id)
        self.set_schedule(session, job_db)

    def from_kwargs(self, key, default=None):
        """
        Helper function to get a value from any extra keyword arguments
        """
        default = default or DEFAULT_KWARGS.get(key)
        return self._initial_kwargs.get(key, default)
