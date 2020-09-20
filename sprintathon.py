from dbo import Dbo
from member import Member


class Sprintathon(Dbo):
    def __init__(self, connection, _id=None, start=None, duration=0) -> None:
        super().__init__(connection)
        self.connection = connection
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
                'INSERT INTO SPRINTATHON(START, DURATION) VALUES(%s, MAKE_INTERVAL(hours => %s)) RETURNING ID, START',
                [start, self.duration])
            result = cursor.fetchone()
            self.id = result[0]
            if not self.start:
                self.start = result[1]
            self.connection.commit()
            return self.id

    def update(self) -> None:
        with self.connection.cursor() as cursor:
            if not self.start:
                start = 'NOW()'
            else:
                start = self.start
            cursor.execute('UPDATE SPRINTATHON SET START=%s, DURATION=MAKE_INTERVAL(hours => %s) WHERE ID=%s RETURNING START',
                           (start, self.duration, self.id))
            result = cursor.fetchone()
            if not self.start:
                self.start = result[0]
            self.connection.commit()

    def delete(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('DELETE FROM SPRINTATHON WHERE ID=%s', [self.id])
            self.connection.commit()

    def find_by_id(self, _id):
        self.id = _id
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT START, DURATION FROM SPRINTATHON WHERE ID=%s', [self.id])
            result = cursor.fetchone()
            self.start = result[0]
            self.duration = result[1]
        return self

    def add_submission(self, submission):
        with self.connection.cursor() as cursor:
            cursor.execute('INSERT INTO SPRINTATHON_SUBMISSION(SPRINTATHON_ID, SUBMISSION_ID) VALUES(%s, %s)', (self.id, submission.id))
            self.connection.commit()

    def get_members(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT DISTINCT MEMBER.ID, MEMBER.NAME, MEMBER.DISCORD_USER_ID FROM MEMBER INNER JOIN SUBMISSION ON '
                'MEMBER.ID=SUBMISSION.MEMBER_ID INNER JOIN SPRINTATHON_SUBMISSION ON '
                'SUBMISSION.ID=SPRINTATHON_SUBMISSION.SUBMISSION_ID WHERE SPRINTATHON_SUBMISSION.SPRINTATHON_ID=%s',
                [self.id])
            result = cursor.fetchall()
            return [Member(self.connection, item[0], item[1], item[2]) for item in result]

    def get_word_count(self, member):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT COALESCE(SUM(SUBMISSION.WORD_COUNT), 0) FROM SUBMISSION INNER JOIN SPRINTATHON_SUBMISSION ON '
                'SUBMISSION.ID=SPRINTATHON_SUBMISSION.SUBMISSION_ID WHERE SPRINTATHON_SUBMISSION.SPRINTATHON_ID=%s AND '
                'SUBMISSION.MEMBER_ID=%s AND SUBMISSION.TYPE=%s',
                (self.id, member.id, 'DELTA'))
            return cursor.fetchone()[0]
