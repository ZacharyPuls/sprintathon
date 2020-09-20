import os

from discord.ext import commands
from dotenv import load_dotenv
import asyncio
import psycopg2

active_sprint_id = 0
active_sprintathon_id = 0

sprintathon_is_active = False
connection = None


def add_member(user_name, discord_user_id):
    with connection.cursor() as cursor:
        cursor.execute('INSERT INTO MEMBER(NAME, DISCORD_USER_ID) VALUES(%s, %s) RETURNING ID', [user_name, discord_user_id])
        result = cursor.fetchone()
        connection.commit()
        return result[0]


def find_member_id(user_name):
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM MEMBER WHERE NAME=%s', [user_name])
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None


def has_member_already_submitted_to(member_id, sprint_id):
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT * FROM SUBMISSION INNER JOIN SPRINT_SUBMISSION ON SUBMISSION.ID=SPRINT_SUBMISSION.SUBMISSION_ID INNER JOIN SPRINT ON SPRINT.ID=SPRINT_SUBMISSION.SPRINT_ID WHERE SPRINT.ID=%s AND SUBMISSION.MEMBER_ID=%s',
            (sprint_id, member_id))
        results = cursor.fetchall()
        if results:
            return True
        else:
            return False


def add_member_to_sprint(member_id, sprint_id):
    with connection.cursor() as cursor:
        cursor.execute('SELECT * FROM SPRINT_MEMBER WHERE MEMBER_ID=%s AND SPRINT_ID=%s', (member_id, sprint_id))
        result = cursor.fetchall()
        # Don't add the same member to a sprint more than once
        if not result:
            cursor.execute('INSERT INTO SPRINT_MEMBER(SPRINT_ID, MEMBER_ID) VALUES(%s, %s)', (sprint_id, member_id))
            connection.commit()


def add_member_submission(sprint_id, user_name, discord_user_id, word_count):
    member_id = find_member_id(user_name)
    if not member_id:
        member_id = add_member(user_name, discord_user_id)
    add_member_to_sprint(member_id, sprint_id)
    with connection.cursor() as cursor:
        if has_member_already_submitted_to(member_id, sprint_id):
            submission_type = 'FINISH'
        else:
            submission_type = 'START'
        cursor.execute('INSERT INTO SUBMISSION(MEMBER_ID, WORD_COUNT, TYPE) VALUES(%s, %s, %s) RETURNING ID',
                       (member_id, word_count, submission_type))
        submission_id = cursor.fetchone()
        cursor.execute('INSERT INTO SPRINT_SUBMISSION(SPRINT_ID, SUBMISSION_ID) VALUES(%s, %s)', (sprint_id,
                       submission_id))
        connection.commit()
        return submission_id


def add_sprintathon_submission(sprintathon_id, submission_id):
    with connection.cursor() as cursor:
        cursor.execute('INSERT INTO SPRINTATHON_SUBMISSION(SPRINTATHON_ID, SUBMISSION_ID) VALUES(%s, %s)',
                       (sprintathon_id, submission_id))
        connection.commit()


def add_sprint(duration_in_minutes):
    with connection.cursor() as cursor:
        cursor.execute(
            'INSERT INTO SPRINT(START, DURATION) VALUES(NOW(), MAKE_INTERVAL(mins => %s)) RETURNING ID',
            [duration_in_minutes])
        result = cursor.fetchone()
        connection.commit()
        return result[0]


def initialize_database(connection_uri):
    global connection
    connection = psycopg2.connect(connection_uri)

    with open('schema.sql') as schema_file:
        schema = schema_file.read()

    with connection.cursor() as cursor:
        cursor.execute(schema)
        connection.commit()


def get_sprint_members(sprint_id):
    with connection.cursor() as cursor:
        cursor.execute('SELECT MEMBER.ID, MEMBER.NAME, MEMBER.DISCORD_USER_ID FROM SPRINT_MEMBER JOIN MEMBER ON SPRINT_MEMBER.MEMBER_ID=MEMBER.ID WHERE SPRINT_MEMBER.SPRINT_ID=%s', [sprint_id])
        return cursor.fetchall()


def get_sprint_member_word_count(sprint_id, member_id, count_type):
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT SUBMISSION.WORD_COUNT FROM SPRINT_SUBMISSION JOIN SUBMISSION ON SPRINT_SUBMISSION.SUBMISSION_ID=SUBMISSION.ID WHERE SPRINT_SUBMISSION.SPRINT_ID=%s AND SUBMISSION.MEMBER_ID=%s AND SUBMISSION.TYPE=%s',
            (sprint_id, member_id, count_type))
        return cursor.fetchone()[0]


def add_sprint_member_word_count(sprint_id, member_id, word_count):
    with connection.cursor() as cursor:
        cursor.execute('INSERT INTO SUBMISSION(MEMBER_ID, WORD_COUNT, TYPE) VALUES(%s, %s, %s) RETURNING ID', (member_id, word_count, 'DELTA'))
        submission_id = cursor.fetchone()
        cursor.execute('INSERT INTO SPRINT_SUBMISSION(SPRINT_ID, SUBMISSION_ID) VALUES(%s, %s)', (sprint_id, submission_id))
        if sprintathon_is_active:
            add_sprintathon_submission(active_sprintathon_id, submission_id)
        connection.commit()


def add_sprintathon(sprintathon_time_in_hours):
    with connection.cursor() as cursor:
        cursor.execute(
            'INSERT INTO SPRINTATHON(START, DURATION) VALUES(NOW(), MAKE_INTERVAL(hours => %s)) RETURNING ID',
            [sprintathon_time_in_hours])
        result = cursor.fetchone()[0]
        connection.commit()
        return result


def get_sum_of_sprint_word_counts_during_sprintathon(sprintathon_id, member_id):
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT COALESCE(SUM(SUBMISSION.WORD_COUNT), 0) FROM SUBMISSION INNER JOIN SPRINTATHON_SUBMISSION ON SUBMISSION.ID=SPRINTATHON_SUBMISSION.SUBMISSION_ID WHERE SPRINTATHON_SUBMISSION.SPRINTATHON_ID=%s AND SUBMISSION.MEMBER_ID=%s AND SUBMISSION.TYPE=%s',
            (sprintathon_id, member_id, 'DELTA'))
        return cursor.fetchone()[0]


def get_sprintathon_members(sprintathon_id):
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT DISTINCT MEMBER.ID, MEMBER.NAME FROM MEMBER INNER JOIN SUBMISSION ON MEMBER.ID=SUBMISSION.MEMBER_ID INNER JOIN SPRINTATHON_SUBMISSION ON SUBMISSION.ID=SPRINTATHON_SUBMISSION.SUBMISSION_ID WHERE SPRINTATHON_SUBMISSION.SPRINTATHON_ID=%s',
            [sprintathon_id])
        return cursor.fetchall()


async def run_sprintathon(ctx, sprinathon_time_in_hours):
    global sprintathon_is_active
    global active_sprintathon_id

    active_sprintathon_id = add_sprintathon(sprinathon_time_in_hours)

    hour_or_hours = 'hour'

    if sprinathon_time_in_hours != 1:
        hour_or_hours = 'hours'

    sprintathon_is_active = True

    response = f'It\'s sprintathon time! Starting a timer for {sprinathon_time_in_hours} {hour_or_hours}, and any sprints ' \
               f'started during this time will add to a running total. Gotta type fast! '

    await ctx.send(response)

    # await asyncio.sleep(sprint_time_in_hours * 60 * 60)
    await asyncio.sleep(180)

    await ctx.send('Sprintathon is up! Here are the results:')

    sprintathon_word_counts = dict()

    for sprintathon_member in get_sprintathon_members(active_sprintathon_id):
        member_id = sprintathon_member[0]
        member_name = sprintathon_member[1]
        sprintathon_word_counts[member_name] = get_sum_of_sprint_word_counts_during_sprintathon(active_sprintathon_id, member_id)

    sprintathon_leaderboard = sorted(sprintathon_word_counts.items(), key=lambda item: item[1], reverse=True)

    await ctx.send(format_leaderboard_string(sprintathon_leaderboard, 'sprintathon'))


async def run_sprint(ctx, sprint_time_in_minutes):
    global active_sprint_id
    active_sprint_id = add_sprint(sprint_time_in_minutes)
    minute_or_minutes = 'minute'

    if sprint_time_in_minutes != 1:
        minute_or_minutes = 'minutes'

    response = f'It\'s sprint time, let\'s get typing! Everyone use \'!sprint [word count]\' with your current ' \
               f'word count to check in. I\'m setting the timer for {sprint_time_in_minutes} {minute_or_minutes}, try and write as much ' \
               f'as you can in the allotted time. When the time is up, I will let you know, and you will have 5 ' \
               f'minutes to check in again with your word count.'

    # TODO: allow members to join for 5 minutes before actually starting the sprint

    await ctx.send(response)

    await asyncio.sleep(sprint_time_in_minutes * 15)

    # TODO: make this 2 minutes, and tag all of the members currently participating in the sprint
    current_sprint_members = ','.join([f'<@{member[2]}>' for member in get_sprint_members(active_sprint_id)])
    await ctx.send(f'Time is up! You have 2 minutes to enter your word count\n    {current_sprint_members} - don\'t forget to check in with your ending word count!')

    await asyncio.sleep(1 * 15)

    await ctx.send('Word count submission time is up!')

    sprint_word_counts = dict()

    for sprint_member in get_sprint_members(active_sprint_id):
        member_id = sprint_member[0]
        word_count = get_sprint_member_word_count(active_sprint_id, member_id, 'FINISH') - get_sprint_member_word_count(active_sprint_id, member_id, 'START')
        add_sprint_member_word_count(active_sprint_id, member_id, word_count)
        # member_name = sprint_member[1]
        member_discord_id = sprint_member[2]
        sprint_word_counts[member_discord_id] = word_count

    sprint_leaderboard = sorted(sprint_word_counts.items(), key=lambda item: item[1], reverse=True)

    await ctx.send(format_leaderboard_string(sprint_leaderboard, 'sprint'))


def format_leaderboard_string(leaderboard, type):
    message = f'**Results for this {type}:**\n'

    for position, result in enumerate(leaderboard):
        st_nd_or_th = ''
        if position == 0:
            st_nd_or_th = 'st'
        elif position == 1:
            st_nd_or_th = 'nd'
        else:
            st_nd_or_th = 'th'

        word_or_words = 'words'

        if result == 1:
            word_or_words = 'word'

        message += f'    {str(position + 1)}{st_nd_or_th}: <@{result[0]}> - {result[1]} {word_or_words}\n'

    return message


def main():
    load_dotenv()
    discord_token = os.environ.get('DISCORD_TOKEN')

    bot = commands.Bot(command_prefix='!')

    connection_string = os.environ.get('PGSQL_CONNECTION_STRING')
    initialize_database(connection_string)

    @bot.event
    async def on_ready():
        print(f'{bot.user.name} has connected to Discord!')

    @bot.command(name='start_sprintathon')
    async def start_sprintathon(ctx, sprint_time_in_hours: int = 24):
        await run_sprintathon(ctx, sprint_time_in_hours)

    @bot.command(name='start_sprint')
    async def start_sprint(ctx, sprint_time_in_minutes: int = 15):
        await run_sprint(ctx, sprint_time_in_minutes)

    @bot.command(name='sprint')
    async def sprint(ctx, word_count: int):
        user_id = ctx.message.author.id
        user_name = ctx.message.author.name
        response = f'{user_name} checked in with {word_count} words!'

        submission_id = add_member_submission(active_sprint_id, user_name, user_id, word_count)

        if sprintathon_is_active:
            add_sprintathon_submission(active_sprintathon_id, submission_id)

        await ctx.send(response)

    bot.run(discord_token)

    connection.close()


if __name__ == '__main__':
    main()
