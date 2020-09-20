from dbo import Dbo


class Member(Dbo):
    def __init__(self, connection, _id=None, name='', discord_user_id=0) -> None:
        super().__init__(connection)
        self.id = _id
        self.name = name
        self.discord_user_id = discord_user_id

    def create(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute('INSERT INTO MEMBER(NAME, DISCORD_USER_ID) VALUES(%s, %s) RETURNING ID',
                           [self.name, self.discord_user_id])
            self.id = cursor.fetchone()[0]
            self.connection.commit()
            return self.id

    def update(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('UPDATE MEMBER SET NAME=%s, DISCORD_USER_ID=%s WHERE ID=%s',
                           (self.name, self.discord_user_id, self.id))
            self.connection.commit()

    def delete(self) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute('DELETE FROM MEMBER WHERE ID=%s', [self.id])
            self.connection.commit()

    def find_by_id(self, _id):
        self.id = _id
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT NAME, DISCORD_USER_ID FROM MEMBER WHERE ID=%s', [self.id])
            result = cursor.fetchone()
            self.name = result[0]
            self.discord_user_id = result[1]
        return self

    def find_by_name(self, name):
        self.name = name
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT ID, DISCORD_USER_ID FROM MEMBER WHERE NAME=%s', [self.name])
            result = cursor.fetchone()
            # TODO: I should really force a UNIQUE constraint on the MEMBER.NAME column, or handle duplicates better...
            if result is None:
                return None
            self.id = result[0]
            self.discord_user_id = result[1]
        return self

    def has_submission_in(self, sprint):
        with self.connection.cursor() as cursor:
            cursor.execute(
                'SELECT COUNT(*) FROM SUBMISSION INNER JOIN SPRINT_SUBMISSION ON SUBMISSION.ID=SPRINT_SUBMISSION.SUBMISSION_ID INNER JOIN SPRINT ON SPRINT_SUBMISSION.SPRINT_ID=SPRINT.ID WHERE SUBMISSION.MEMBER_ID=%s AND SPRINT.ID=%s',
                (self.id, sprint.id))
            result = cursor.fetchone()
            return int(result[0]) > 0
