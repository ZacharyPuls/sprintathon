import logging

from dbo import Dbo
from member import Member
from submission import Submission


class Sprint(Dbo):
    def __init__(self, connection, _id=None, start=None, duration=0) -> None:
        self.logger = logging.getLogger('sprintathon.Sprint')
        super().__init__(connection)
        self.id = _id
        self.start = start
        self.duration = duration

    def create(self) -> int:
        with self.connection.cursor() as cursor:
            if not self.start:
                start = 'NOW()'
            else:
                start = self.start
            cursor.execute(
                'INSERT INTO SPRINT(START, DURATION) VALUES(%s, MAKE_INTERVAL(mins => %s)) RETURNING ID, START',
                [start, self.duration])
            result = cursor.fetchone()
            self.id = result[0]
            if not self.start:
                self.start = result[1]
            self.connection.commit()
            self.logger.debug('Inserting %s into database.', self)
            return self.id

    def update(self) -> None:
        with self.connection.cursor() as cursor:
            if not self.start:
                start = 'NOW()'
            else:
                start = self.start
            cursor.execute('UPDATE SPRINT SET START=%s, DURATION=MAKE_INTERVAL(mins => %s) WHERE ID=%s RETURNING START',
                           (start, self.duration, self.id))
            result = cursor.fetchone()
            if not self.start:
                self.start = result[0]
            self.connection.commit()
            self.logger.debug('Updating %s in database.', self)

    def delete(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('DELETE FROM SPRINT WHERE ID=%s', [self.id])
            self.connection.commit()
            self.logger.debug('Deleting %s from database.', self)

    def find_by_id(self, _id):
        self.id = _id
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT START, DURATION FROM SPRINT WHERE ID=%s', [self.id])
            result = cursor.fetchone()
            self.start = result[0]
            self.duration = result[1]
        return self

    def get_members(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT MEMBER.ID, MEMBER.NAME, MEMBER.DISCORD_USER_ID FROM MEMBER INNER JOIN SPRINT_MEMBER ON MEMBER.ID=SPRINT_MEMBER.MEMBER_ID INNER JOIN SPRINT ON SPRINT_MEMBER.SPRINT_ID=SPRINT.ID WHERE SPRINT.ID=%s',
                [self.id])
            result = cursor.fetchall()
            return [Member(self.connection, item[0], item[1], item[2]) for item in result]

    def add_member(self, member):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT * FROM SPRINT_MEMBER WHERE MEMBER_ID=%s AND SPRINT_ID=%s', (member.id, self.id))
            result = cursor.fetchall()
            # Don't add the same member to a sprint more than once
            if not result:
                cursor.execute('INSERT INTO SPRINT_MEMBER(SPRINT_ID, MEMBER_ID) VALUES(%s, %s)', (self.id, member.id))
                self.connection.commit()

    def get_submissions(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT SUBMISSION.ID, SUBMISSION.MEMBER_ID, SUBMISSION.WORD_COUNT, SUBMISSION.TYPE FROM SUBMISSION INNER JOIN SPRINT_SUBMISSION ON SUBMISSION.ID=SPRINT_SUBMISSION.SUBMISSION_ID INNER JOIN SPRINT ON SPRINT_SUBMISSION.SPRINT_ID=SPRINT.ID WHERE SPRINT.ID=%s',
                [self.id])
            result = cursor.fetchall()
            return [
                Submission(self.connection, item[0], Member(connection=self.connection).find_by_id(item[1]), item[2],
                           item[3]) for item in result]

    def add_submission(self, submission):
        with self.connection.cursor() as cursor:
            if not submission.id:
                submission.create()
            cursor.execute('INSERT INTO SPRINT_SUBMISSION(SPRINT_ID, SUBMISSION_ID) VALUES(%s, %s)',
                           (self.id, submission.id))
            self.connection.commit()

    def __repr__(self) -> str:
        return f'Sprint{{id={self.id},start={self.start},duration={self.duration}}}'

    # def get_word_count(self, member, count_type):
    #     with self.connection.cursor() as cursor:
    #         cursor.execute(
    #             'SELECT SUBMISSION.WORD_COUNT FROM SPRINT_SUBMISSION JOIN SUBMISSION ON '
    #             'SPRINT_SUBMISSION.SUBMISSION_ID=SUBMISSION.ID WHERE SPRINT_SUBMISSION.SPRINT_ID=%s '
    #             'AND SUBMISSION.MEMBER_ID=%s AND SUBMISSION.TYPE=%s',
    #             (self.id, member.id, count_type))
    #         return cursor.fetchone()[0]
