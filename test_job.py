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


j = Job(
    'test job',
    start_date=datetime(2018, 8, 1),  # '2018-08-01',
    schedule='0 0 * * *',
)

t1 = Task(action=fn_hello)
j.add_task(t1)


t2 = Task(action=fn_exception)
j.set_dependency(t1, t2)
j.add_task(t2)
