from datetime import datetime
from ratpiz import Task, Job
from time import sleep


def fn_hello(context):
    sleep(0.1)
    print('hello')


def fn_exception(context):

    sleep(0.1)
    context.retry_task('retry...')
    print('exception')
    raise Exception('Exception raised')


job = Job(
    'test job',
    start_date=datetime(2018, 8, 1),  # '2018-08-01',
    schedule='0 0 * * *',
    # retries=4,
)

task_hello = Task(action=fn_hello)
job.add_task(task_hello)


task_exception = Task(action=fn_exception)  # , retries=1)
job.add_task(task_exception)

job.set_dependency(task_hello, task_exception)
