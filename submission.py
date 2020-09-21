import logging

from dbo import Dbo
from member import Member


class Submission(Dbo):
    def __init__(self, connection, _id=None, member=None, word_count=0, _type='') -> None:
        self.logger = logging.getLogger('sprintathon.Submission')
        super().__init__(connection)
        self.id = _id
        self.member = member
        self.word_count = word_count
        self.type = _type

    def create(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute('INSERT INTO SUBMISSION(MEMBER_ID, WORD_COUNT, TYPE) VALUES(%s, %s, %s) RETURNING ID',
                           [self.member.id, self.word_count, self.type])
            self.id = cursor.fetchone()[0]
            self.connection.commit()
            self.logger.debug('Inserting %s into database.', self)
            return self.id

    def update(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('UPDATE SUBMISSION SET MEMBER_ID=%s, WORD_COUNT=%s, TYPE=%s WHERE ID=%s',
                           (self.member.id, self.word_count, self.type, self.id))
            self.connection.commit()
            self.logger.debug('Updating %s in database.', self)

    def delete(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('DELETE FROM SUBMISSION WHERE ID=%s', [self.id])
            self.connection.commit()
            self.logger.debug('Deleting %s from database.', self)

    def find_by_id(self, _id):
        self.id = _id
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT MEMBER_ID, WORD_COUNT, TYPE FROM SUBMISSION WHERE SUBMISSION.ID=%s', [self.id])
            result = cursor.fetchone()
            self.member = Member(connection=self.connection).find_by_id(result[0])
            self.word_count = result[1]
            self.type = result[2]
        return self

    @staticmethod
    def find_all_by_member(connection, member):
        with connection.cursor() as cursor:
            cursor.execute('SELECT ID, WORD_COUNT, TYPE FROM SUBMISSION WHERE MEMBER_ID=%s', [member.id])
            result = cursor.fetchall()
            return [Submission(connection, item[0], member, item[1], item[2]) for item in result]

    @staticmethod
    def find_all_by_member_and_sprint(connection, member, sprint):
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT SUBMISSION.ID, SUBMISSION.WORD_COUNT, SUBMISSION.TYPE FROM SUBMISSION INNER JOIN SPRINT_SUBMISSION ON SUBMISSION.ID=SPRINT_SUBMISSION.SUBMISSION_ID INNER JOIN SPRINT ON SPRINT.ID=SPRINT_SUBMISSION.SPRINT_ID WHERE SUBMISSION.MEMBER_ID=%s AND SPRINT.ID=%s',
                (member.id, sprint.id))
            result = cursor.fetchall()
            return [Submission(connection, item[0], member, item[1], item[2]) for item in result]

    def __repr__(self) -> str:
        return f'Submission{{id={self.id},member={repr(self.member)},word_count={self.word_count},type={self.type}}}'
