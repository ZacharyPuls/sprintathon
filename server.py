import logging

from dbo import Dbo
import member
import sprint
import sprintathon


class Server(Dbo):
    def __init__(self, connection, _id=None, name='', discord_guild_id=None) -> None:
        self.logger = logging.getLogger('sprintathon.Server')
        super().__init__(connection)
        self.id = _id
        self.name = name
        self.discord_guild_id = discord_guild_id

    def create(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute('INSERT INTO SERVER(NAME, DISCORD_GUILD_ID) VALUES(%s, %s) RETURNING ID',
                           [self.name, self.discord_guild_id])
            result = cursor.fetchone()
            self.id = result[0]
            self.connection.commit()
            self.logger.debug('Inserting %s into database.', self)
            return self.id

    def update(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('UPDATE SERVER SET NAME = %s, DISCORD_GUILD_ID = %s WHERE ID=%s',
                           (self.name, self.discord_guild_id, self.id))
            self.connection.commit()
            self.logger.debug('Updating %s in database.', self)

    def delete(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('DELETE FROM SERVER WHERE ID=%s', [self.id])
            self.connection.commit()
            self.logger.debug('Deleting %s from database.', self)

    def find_by_id(self, _id):
        self.id = _id
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT NAME, DISCORD_GUILD_ID FROM SERVER WHERE ID=%s', [self.id])
            result = cursor.fetchone()
            self.name = result[0]
            self.discord_guild_id = result[1]
        return self

    def find_or_create(self):
        if not self.name or not self.discord_guild_id:
            self.logger.error(f'Attempted to call Server.find_or_create() without setting Server.name and '
                              f'Server.discord_guild_id.')
            return None

        with self.connection.cursor() as cursor:
            cursor.execute('SELECT ID FROM SERVER WHERE NAME=%s AND DISCORD_GUILD_ID=%s',
                           (self.name, self.discord_guild_id))
            result = cursor.fetchone()

            if result is None:
                self.create()
            else:
                self.id = result[0]
            return self

    def add_member(self, _member):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT * FROM SERVER_MEMBER WHERE MEMBER_ID=%s AND SERVER_ID=%s', (_member.id, self.id))
            result = cursor.fetchall()
            # Don't add the same member to a server more than once
            if not result:
                cursor.execute('INSERT INTO SERVER_MEMBER(SERVER_ID, MEMBER_ID) VALUES(%s, %s)', (self.id, _member.id))
                self.connection.commit()

    def get_members(self):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT MEMBER.ID, MEMBER.NAME, MEMBER.DISCORD_USER_ID FROM MEMBER '
                           'INNER JOIN SERVER_MEMBER ON MEMBER.ID=SERVER_MEMBER.MEMBER_ID '
                           'INNER JOIN SERVER ON SERVER_MEMBER.SERVER_ID=SERVER.ID WHERE SERVER.ID=%s', [self.id])
            result = cursor.fetchall()
            return [member.Member(self.connection, item[0], item[1], item[2]) for item in result]

    def get_sprints(self):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT ID, START, DURATION, SERVER_ID FROM SPRINT WHERE SERVER_ID=%s', [self.id])
            result = cursor.fetchall()
            return [sprint.Sprint(self.connection, item[0], item[1], item[2], item[3]) for item in result]

    def get_sprintathons(self):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT ID, START, DURATION, SERVER_ID FROM SPRINTATHON WHERE SERVER_ID=%s', [self.id])
            result = cursor.fetchall()
            return [sprintathon.Sprintathon(self.connection, item[0], item[1], item[2], item[3]) for item in result]

    def __repr__(self) -> str:
        return f'Server{{id={self.id},name={self.name},discord_guild_id={self.discord_guild_id}}}'
