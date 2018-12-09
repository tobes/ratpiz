import json
import logging
import os.path

from subprocess import Popen, PIPE
from threading import Thread
from time import sleep

from ratpiz import db

from ratpiz.constants import (
    STATE_PENDING,
)


RUNNER_PATH = os.path.join(os.path.dirname(__file__), 'process_runner.py')

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class CommandRunner(Thread):
    """
    Run the command in a process so that it is isolated.
    Monitor the process and log output.
    """

    def __init__(self, cmd, uuid):
        Thread.__init__(self)
        self.cmd = cmd
        self.uuid = uuid

    def run(self):
        """
        Run the command.
        """
        session = db.Session()
        uuid = self.uuid
        # run the command
        process = Popen(self.cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)

        # monitor until it finishes
        while process.poll() is None:
            if uuid:
                db.Event.set_heartbeat(session, uuid)
            # sleep so as not to use too many resources
            sleep(1)

        # logging
        print('process')
        print(process.stdout.read().decode())
        print(process.stderr.read().decode())
        print(process.returncode)


def run_command(payload=None):

    path = payload.get('path')
    uuid = payload.get('uuid')

    cmd = [
        'python',
        RUNNER_PATH,
    ]
    if path:
        cmd += ['-e', path]
    if payload:
        cmd += ['--json', json.dumps(payload)]

    print(' '.join(cmd))
    # run command in a thread
    t = CommandRunner(cmd, uuid)
    t.start()


def set_schedule(session, job_db, job):
    """
    see if this job needs to be scheduled to run.  if so the we add
    ourself.
    """
    log.info('set schedule')

    # are we already scheduled?
    if job_db.scheduled(session):
        log.info('no need to schedule')
        return

    # when are we next due to run?
    base_date = job_db.last_run or job.start_date
    schedule = croniter(job.schedule, base_date)
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
    log.info('schedule for %s', next_schedule_dt)
    # add job to schedule so that we are run
    job = db.JobRun.add_run(
        session, next_schedule_dt, job_id=job_db.job_id
    )
    log.info('added %s', job.uuid)
    db.Event.add_run(
            session,
            due_time=job.due_time,
            event_type=TYPE_JOB,
            uuid=job.uuid,
    )
if __name__ == '__main__':

    # for testing we register a job
    payload = {
        'action': 'register',
        'path': 'test_job.py',
    }
    run_command(payload=payload)

    session = db.Session()
    try:
        while True:
            # any events need to run?
            next_event = db.Event.next_scheduled(session, state=STATE_PENDING)
            if next_event:
                print('schedule Event')
                payload = {
                    'action': 'run',
                    'event_type': next_event.event_type,
                    'uuid': next_event.uuid,
                }
                run_command(payload=payload)

            # clear any jobs that are pending
            # db.JobRun.clear_pending(session)

            # sleep so as not to use too many resources
            sleep(1)

    except KeyboardInterrupt:
        print('stopping...')
    session.close()
