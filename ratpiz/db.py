import os.path

from datetime import timedelta, datetime

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Boolean,
    TIMESTAMP,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

# create connection string
path = os.path.join(os.path.dirname(__file__), 'test.sqlite')
connection_str = 'sqlite:///{}'.format(path)


engine = create_engine(connection_str, echo=False)
Session = sessionmaker(bind=engine)


Base = declarative_base()

PENDING_TIMEOUT = timedelta(seconds=1)

# states
PENDING = 'pending'
WAITING = 'waiting'
RETRY = 'retry'
SUCCESS = 'success'
FAIL = 'fail'


class Job(Base):
    __tablename__ = 'job'

    job_id = Column(Integer, primary_key=True)
    name = Column(String)
    path = Column(String)
    python_path = Column(String)
    last_run = Column(TIMESTAMP)
    active = Column(Boolean, default=True)
    date_creation = Column(TIMESTAMP, default=func.now())
    last_updated = Column(TIMESTAMP, onupdate=func.now())

    @classmethod
    def get_by_id(cls, session, job_id):
        job = (
                session.query(cls)
                .filter(cls.job_id == job_id)
                .first()
        )
        return job

    @classmethod
    def register(cls, session, job):
        j = (
                session.query(Job)
                .filter(Job.name == job.name)
                .first()
        )
        if j:
            print('job exists..')
            return j
        data = {
            'name': job.name,
            'path': job.path,
            'python_path': job.python_path,
            'date_creation': datetime.today(),
        }
        j = Job(**data)
        session.add(j)
        session.commit()
        print('registered job')
        return j

    def scheduled(self, session):
        return JobRun.get_scheduled(session, job_id=self.job_id)


class RunBase:
    """
    Mix-in class to add extra functionality to JobRun and TaskRun
    """

    @classmethod
    def clear_pending(cls, session):
        since = datetime.utcnow() - PENDING_TIMEOUT
        all_pending = (
                session.query(cls)
                .filter(cls.active == True)  # noqa
                .filter(cls.completed == False)  # noqa
                .filter(cls.state == PENDING)
                .filter(cls.last_updated <= since)
                .with_for_update()
        )
        for pending in all_pending.all():
            pending.state = WAITING
            session.add(pending)
        session.commit()

    @classmethod
    def next_scheduled(cls, session, state=None):
        scheduled = (
                session.query(cls)
                .filter(cls.active == True)  # noqa
                .filter(cls.completed == False)  # noqa
                .filter(cls.state.in_([WAITING, RETRY]))
                .filter(cls.due_time <= func.now())
                .order_by(cls.due_time)
        )
        if state:
            scheduled = scheduled.with_for_update()
            next_scheduled = scheduled.first()
            if next_scheduled:
                next_scheduled.state = state
                session.add(next_scheduled)
                session.commit()
        else:
            next_scheduled = scheduled.first()
        return next_scheduled

    @classmethod
    def get_by_id(cls, session, id_):
        job_run = (
                session.query(cls)
                .filter(getattr(cls, cls.__primary_key__) == id_)
                .first()
        )
        return job_run

    @classmethod
    def get_scheduled(cls, session, **kwargs):
        scheduled = (
                session.query(func.max(cls.due_time))
                .filter(cls.active == True)  # noqa
                .filter(cls.completed == False)  # noqa
        )
        for key, value in kwargs.items():
            scheduled = scheduled.filter(getattr(cls, key) == value)
        scheduled = scheduled.all()
        if scheduled[0][0] is None:
            return None
        return scheduled

    @classmethod
    def add_run(cls, session, due_time, **kwargs):
        print('add_run')
        print(kwargs)
        # printmax_retries >= ('@@ %s %s' % (job_id, due_time))
        query = (
                session.query(cls)
        )

        if cls == JobRun:
            query = query.filter(cls.due_time == due_time)
        for key, value in kwargs.items():
            query = query.filter(getattr(cls, key) == value)
        result = query.first()
        if result:
            print('exists..')
            return result
        data = {
            'due_time': due_time,
            'state': WAITING,
        }
        if kwargs:
            data.update(kwargs)
        run = cls(**data)
        session.add(run)
        session.commit()

    def get_run_obj(self, session):
        return self.__child_object__.get_by_id(
                session,
                getattr(self, self.__child_key__)
        )

    def get_job(self, session):
        return Job.get_by_id(session, self.job_id)

    def get_job_run(self, session):
        return JobRun.get_by_id(session, self.job_run_id)

    def complete(self, session, state=SUCCESS):
        if state == RETRY:
            raise
        self.completed = True
        self.state = state
        session.add(self)
        job = self.get_run_obj(session)
        job.last_run = self.due_time
        session.add(job)
        session.commit()

    def set_state(self, session, state):
        if state == RETRY:
            raise
        self.state = state
        session.add(self)
        session.commit()


class JobRun(Base, RunBase):
    __tablename__ = 'job_run'

    __primary_key__ = 'job_run_id'
    __child_object__ = Job
    __child_key__ = 'job_id'

    job_run_id = Column(Integer, primary_key=True)
    job_id = Column(Integer)
    state = Column(String)
    active = Column(Boolean, default=True)
    completed = Column(Boolean, default=False)
    due_time = Column(TIMESTAMP)
    date_creation = Column(TIMESTAMP, default=func.now())
    last_updated = Column(TIMESTAMP, onupdate=func.now())

    def get_completed_tasks(self, session):
        tasks = (
                session
                .query(TaskRun)
                .filter(TaskRun.job_run_id == self.job_run_id)
                .filter(TaskRun.completed == True)  # noqa
                .filter(TaskRun.active == True)  # noqa
        )
        return tasks.all()


class TaskRun(Base, RunBase):
    __tablename__ = 'task_run'

    __primary_key__ = 'task_run_id'
    __child_object__ = JobRun
    __child_key__ = 'job_run_id'

    task_run_id = Column(Integer, primary_key=True)
    job_run_id = Column(Integer)
    job_id = Column(Integer)
    task_name = Column(String)
    retries = Column(Integer, default=0)
    state = Column(String)
    active = Column(Boolean, default=True)
    completed = Column(Boolean, default=False)
    due_time = Column(TIMESTAMP)
    date_creation = Column(TIMESTAMP, default=func.now())
    last_updated = Column(TIMESTAMP, onupdate=func.now())

    def set_retry(self, session, retry_delay):
        retry_time = datetime.utcnow() + timedelta(seconds=retry_delay)
        task_run = (
                session.query(TaskRun)
                .filter(TaskRun.task_run_id == self.task_run_id)
                .with_for_update()
        ).one()
        task_run.state = RETRY
        task_run.due_time = retry_time
        task_run.retries += 1
        session.add(task_run)
        session.commit()


Base.metadata.create_all(engine)
