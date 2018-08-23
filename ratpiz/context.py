from ratpiz import exceptions


class Context:
    """
    Object that a task action receives when it is called.
    Contains some useful methods as well as any payload.
    """

    def __init__(self, task_run=None, session=None):
        self._task_run = task_run
        self._session = session
        self._context_dict = None

        job_run = task_run.get_job_run(session)
        self._job_run = job_run

        self._context_dict = {
            'due_time': job_run.due_time,
            'attempt': task_run.retries + 1,

        }

    def __getitem__(self, key):
        """
        Access items via context[key]
        """

        return self._context_dict[key]

    def __repr__(self):
        return '<Context %r %r>' % (self._task_run, self._context_dict)

    def fail_task(self, message):
        """
        Task has failed
        """
        raise exceptions.Fail(message)

    def retry_task(self, message):
        """
        Task should retry
        """
        raise exceptions.Retry(message)
