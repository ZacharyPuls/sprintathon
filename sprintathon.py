import logging

from dbo import Dbo
from member import Member
import server


class Sprintathon(Dbo):
    def __init__(self, connection, _id=None, start=None, duration=0, _server=None, active=True,
                 discord_channel_id=None) -> None:
        self.logger = logging.getLogger('sprintathon.Sprintathon')
        super().__init__(connection)
        self.connection = connection
        self.id = _id
        self.start = start
        self.duration = duration
        self.server = _server
        self.active = active
        self.discord_channel_id = discord_channel_id

    def create(self) -> int:
        with self.connection.cursor() as cursor:
            if not self.start:
                start = 'NOW()'
            else:
                start = self.start
            cursor.execute(
                'INSERT INTO SPRINTATHON(START, DURATION, SERVER_ID, ACTIVE, DISCORD_CHANNEL_ID) '
                'VALUES(%s, MAKE_INTERVAL(hours => %s), %s, %s, %s) RETURNING ID, START',
                [start, self.duration, self.server.id, self.active, self.discord_channel_id])
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
            cursor.execute('UPDATE SPRINTATHON SET START = %s, DURATION = MAKE_INTERVAL(hours => %s), SERVER_ID = %s, '
                           'ACTIVE = %s, DISCORD_CHANNEL_ID = %s WHERE ID=%s RETURNING START',
                           (start, self.duration, self.server.id, self.active, self.discord_channel_id, self.id))
            result = cursor.fetchone()
            if not self.start:
                self.start = result[0]
            self.connection.commit()
            self.logger.debug('Updating %s in database.', self)

    def delete(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('DELETE FROM SPRINTATHON WHERE ID=%s', [self.id])
            self.connection.commit()
            self.logger.debug('Deleting %s from database.', self)

    def fetch(self):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT START, DURATION, SERVER_ID, ACTIVE, DISCORD_CHANNEL_ID '
                           'FROM SPRINTATHON WHERE ID=%s', [self.id])
            result = cursor.fetchone()
            self.start = result[0]
            self.duration = int(result[1].total_seconds() // 3600)
            self.server = server.Server(self.connection).find_by_id(result[2])
            self.active = result[3]
            self.discord_channel_id = result[4]

    def find_by_id(self, _id):
        if _id is None:
            return None
        self.id = _id
        self.fetch()
        return self

    def add_submission(self, submission):
        with self.connection.cursor() as cursor:
            cursor.execute('INSERT INTO SPRINTATHON_SUBMISSION(SPRINTATHON_ID, SUBMISSION_ID) '
                           'VALUES(%s, %s)', (self.id, submission.id))
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

    @staticmethod
    def get_active(connection):
        with connection.cursor() as cursor:
            cursor.execute('SELECT ID, START, DURATION, SERVER_ID, DISCORD_CHANNEL_ID '
                           'FROM SPRINTATHON WHERE ACTIVE=TRUE')
            result = cursor.fetchall()
            return [
                Sprintathon(connection, item[0], item[1], int(item[2].total_seconds() // 3600),
                            server.Server(connection).find_by_id(item[3]), True, item[4]) for item in result]

    @staticmethod
    def get_active_for_channel(connection, channel_id):
        with connection.cursor() as cursor:
            cursor.execute('SELECT ID, START, DURATION, SERVER_ID FROM SPRINTATHON '
                           'WHERE ACTIVE=TRUE AND DISCORD_CHANNEL_ID=%s '
                           'ORDER BY START DESC LIMIT 1', [channel_id])
            result = cursor.fetchone()
            if result is None:
                return None
            return Sprintathon(connection, result[0], result[1], int(result[2].total_seconds() // 3600),
                               server.Server(connection).find_by_id(result[3]), True, channel_id)

    def __repr__(self) -> str:
        return f'Sprintathon{{id={self.id},start={self.start},duration={self.duration},server={self.server},' \
               f'discord_channel_id={self.discord_channel_id}}}'
