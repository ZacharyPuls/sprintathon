import logging

import sprintathon
from dbo import Dbo
import member
import submission
import server


class Sprint(Dbo):
    def __init__(self, connection, _id=None, start=None, duration=0, _server=None, active=True, _sprintathon=None,
                 discord_channel_id=None) -> None:
        self.logger = logging.getLogger('sprintathon.Sprint')
        super().__init__(connection)
        self.id = _id
        self.start = start
        self.duration = duration
        self.server = _server
        self.active = active
        self.sprintathon = _sprintathon
        self.discord_channel_id = discord_channel_id

    def create(self) -> int:
        with self.connection.cursor() as cursor:
            if not self.start:
                start = 'NOW()'
            else:
                start = self.start
            sprintathon_id = None
            if self.sprintathon is not None:
                sprintathon_id = self.sprintathon.id
            cursor.execute(
                'INSERT INTO SPRINT(START, DURATION, SERVER_ID, SPRINTATHON_ID, ACTIVE, DISCORD_CHANNEL_ID) '
                'VALUES(%s, MAKE_INTERVAL(mins => %s), %s, %s, %s, %s) RETURNING ID, START',
                [start, self.duration, self.server.id, sprintathon_id, self.active, self.discord_channel_id])
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
            sprintathon_id = None
            if self.sprintathon is not None:
                sprintathon_id = self.sprintathon.id
            cursor.execute(
                'UPDATE SPRINT SET START = %s, DURATION = MAKE_INTERVAL(mins => %s), SERVER_ID = %s, '
                'SPRINTATHON_ID = %s, ACTIVE = %s, DISCORD_CHANNEL_ID = %s WHERE ID=%s RETURNING START',
                (start, self.duration, self.server.id, sprintathon_id, self.active, self.discord_channel_id, self.id))
            result = cursor.fetchone()
            if not self.start:
                self.start = result[0]
            self.connection.commit()
            self.logger.debug('Updating %s in database.', self)

    def fetch(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT START, DURATION, SERVER_ID, SPRINTATHON_ID, ACTIVE, DISCORD_CHANNEL_ID FROM SPRINT '
                           'WHERE ID=%s', [self.id])
            result = cursor.fetchone()
            self.start = result[0]
            self.duration = int(result[1].total_seconds() // 60)
            self.server = server.Server(self.connection).find_by_id(result[2])
            self.sprintathon = sprintathon.Sprintathon(self.connection).find_by_id(result[3])
            self.active = result[4]
            self.discord_channel_id = result[5]

    def delete(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('DELETE FROM SPRINT WHERE ID=%s', [self.id])
            self.connection.commit()
            self.logger.debug('Deleting %s from database.', self)

    def find_by_id(self, _id):
        self.id = _id
        self.fetch()
        return self

    def get_members(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT MEMBER.ID, MEMBER.NAME, MEMBER.DISCORD_USER_ID FROM MEMBER '
                'INNER JOIN SPRINT_MEMBER ON MEMBER.ID=SPRINT_MEMBER.MEMBER_ID '
                'INNER JOIN SPRINT ON SPRINT_MEMBER.SPRINT_ID=SPRINT.ID WHERE SPRINT.ID=%s',
                [self.id])
            result = cursor.fetchall()
            return [member.Member(self.connection, item[0], item[1], item[2]) for item in result]

    def add_member(self, _member):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT * FROM SPRINT_MEMBER WHERE MEMBER_ID=%s AND SPRINT_ID=%s', (_member.id, self.id))
            result = cursor.fetchall()
            # Don't add the same member to a sprint more than once
            if not result:
                cursor.execute('INSERT INTO SPRINT_MEMBER(SPRINT_ID, MEMBER_ID) VALUES(%s, %s)', (self.id, _member.id))
                self.connection.commit()

    def get_submissions(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT SUBMISSION.ID, SUBMISSION.MEMBER_ID, SUBMISSION.WORD_COUNT, SUBMISSION.TYPE FROM SUBMISSION '
                'INNER JOIN SPRINT_SUBMISSION ON SUBMISSION.ID=SPRINT_SUBMISSION.SUBMISSION_ID '
                'INNER JOIN SPRINT ON SPRINT_SUBMISSION.SPRINT_ID=SPRINT.ID WHERE SPRINT.ID=%s',
                [self.id])
            result = cursor.fetchall()
            return [submission.Submission(self.connection, item[0],
                                          member.Member(connection=self.connection).find_by_id(item[1]), item[2],
                                          item[3]) for item in result]

    def add_submission(self, _submission):
        with self.connection.cursor() as cursor:
            if not _submission.id:
                _submission.create()
            cursor.execute('INSERT INTO SPRINT_SUBMISSION(SPRINT_ID, SUBMISSION_ID) VALUES(%s, %s)',
                           (self.id, _submission.id))
            self.connection.commit()
            if self.sprintathon is not None:
                self.sprintathon.add_submission(_submission)

    @staticmethod
    def get_active(connection):
        with connection.cursor() as cursor:
            cursor.execute('SELECT ID, START, DURATION, SERVER_ID, ACTIVE, SPRINTATHON_ID, DISCORD_CHANNEL_ID '
                           'FROM SPRINT WHERE ACTIVE=TRUE')
            result = cursor.fetchall()
            return [Sprint(connection, item[0], item[1], int(item[2].total_seconds() // 60),
                           server.Server(connection).find_by_id(item[3]), item[4],
                           sprintathon.Sprintathon(connection).find_by_id(item[5]), item[6]) for item in result]

    @staticmethod
    def get_most_recent_active(connection, _server, channel_id):
        with connection.cursor() as cursor:
            cursor.execute('SELECT ID, START, DURATION, SPRINTATHON_ID FROM SPRINT '
                           'WHERE ACTIVE=TRUE AND SERVER_ID = %s AND DISCORD_CHANNEL_ID = %s '
                           'ORDER BY START DESC LIMIT 1', [_server.id, channel_id])
            result = cursor.fetchone()
            if result is None:
                return None
            return Sprint(connection, result[0], result[1], int(result[2].total_seconds() // 60), _server, True,
                          sprintathon.Sprintathon(connection).find_by_id(result[3]), channel_id)

    def __repr__(self) -> str:
        return f'Sprint{{id={self.id},start={self.start},duration={self.duration},server={self.server},' \
               f'active={self.active},sprintathon={self.sprintathon},discord_channel_id={self.discord_channel_id}}}'

    def time_is_up_message(self) -> str:
        current_sprint_members = ','.join([f'<@{_member.discord_user_id}>' for _member in self.get_members()])
        return f':alarm_clock: :alarm_clock: :alarm_clock: Time is up! You have 7 minutes to enter your word count. ' \
               f'Type !sprint [word count] with your ending word count to conclude this sprint.  :alarm_clock: ' \
               f':alarm_clock: :alarm_clock:\n    {current_sprint_members} - don\'t forget to check in with your ' \
               f'ending word count! '
    # def get_word_count(self, member, count_type):
    #     with self.connection.cursor() as cursor:
    #         cursor.execute(
    #             'SELECT SUBMISSION.WORD_COUNT FROM SPRINT_SUBMISSION JOIN SUBMISSION ON '
    #             'SPRINT_SUBMISSION.SUBMISSION_ID=SUBMISSION.ID WHERE SPRINT_SUBMISSION.SPRINT_ID=%s '
    #             'AND SUBMISSION.MEMBER_ID=%s AND SUBMISSION.TYPE=%s',
    #             (self.id, member.id, count_type))
    #         return cursor.fetchone()[0]
