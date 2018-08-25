# states
STATE_PENDING = 'pending'
STATE_RUNNING = 'running'
STATE_WAITING = 'waiting'
STATE_RETRY = 'retry'
STATE_SUCCESS = 'success'
STATE_FAIL = 'fail'

STATES_RUNNABLE = [STATE_WAITING, STATE_RETRY]
# event types
TYPE_JOB = 'job'
TYPE_TASK = 'task'
