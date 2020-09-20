import os

from discord.ext import commands
from dotenv import load_dotenv
import asyncio
import psycopg2

from member import Member
from submission import Submission
from sprint import Sprint
from sprintathon import Sprintathon

active_sprint: Sprint
active_sprintathon: Sprintathon

sprintathon_is_active = False
connection = None


def add_sprintathon_submission(sprintathon_id, submission_id):
    with connection.cursor() as cursor:
        cursor.execute('INSERT INTO SPRINTATHON_SUBMISSION(SPRINTATHON_ID, SUBMISSION_ID) VALUES(%s, %s)',
                       (sprintathon_id, submission_id))
        connection.commit()


def initialize_database(connection_uri):
    global connection
    connection = psycopg2.connect(connection_uri)

    with open('schema.sql') as schema_file:
        schema = schema_file.read()

    with connection.cursor() as cursor:
        cursor.execute(schema)
        connection.commit()


async def run_sprintathon(ctx, sprintathon_time_in_hours):
    global sprintathon_is_active
    global active_sprintathon

    active_sprintathon = Sprintathon(connection=connection, duration=sprintathon_time_in_hours)
    active_sprintathon.create()

    hour_or_hours = 'hour'

    if sprintathon_time_in_hours != 1:
        hour_or_hours = 'hours'

    sprintathon_is_active = True

    response = f'It\'s sprintathon time! Starting a timer for {sprintathon_time_in_hours} {hour_or_hours}, and any sprints ' \
               f'started during this time will add to a running total. Gotta type fast! '

    await ctx.send(response)

    # await asyncio.sleep(sprint_time_in_hours * 60 * 60)
    await asyncio.sleep(90)

    await ctx.send('Sprintathon is up! Here are the results:')

    sprintathon_word_counts = dict()

    for sprintathon_member in active_sprintathon.get_members():
        sprintathon_word_counts[sprintathon_member.discord_user_id] = active_sprintathon.get_word_count(sprintathon_member)

    sprintathon_leaderboard = sorted(sprintathon_word_counts.items(), key=lambda item: item[1], reverse=True)

    await ctx.send(format_leaderboard_string(sprintathon_leaderboard, 'sprintathon'))


async def run_sprint(ctx, sprint_time_in_minutes):
    global active_sprint
    active_sprint = Sprint(connection=connection, duration=sprint_time_in_minutes)
    active_sprint.create()
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
    current_sprint_members = ','.join([f'<@{member.discord_user_id}>' for member in active_sprint.get_members()])
    await ctx.send(f'Time is up! You have 2 minutes to enter your word count\n    {current_sprint_members} - don\'t forget to check in with your ending word count!')

    await asyncio.sleep(1 * 15)

    await ctx.send('Word count submission time is up!')

    sprint_word_counts = dict()

    for sprint_member in active_sprint.get_members():
        submissions = Submission.find_all_by_member_and_sprint(connection, sprint_member, active_sprint)
        finish_word_count = next((item.word_count for item in submissions if item.type == 'FINISH'), None)
        start_word_count = next((item.word_count for item in submissions if item.type == 'START'), None)
        if not start_word_count or not finish_word_count:
            await ctx.send(f'Sprint member <@{sprint_member.discord_user_id}> forgot to submit their final word '
                           f'count! Skipping user for leaderboard calculations.')
            continue

        if finish_word_count < start_word_count:
            await ctx.send(f'Sprint member <@{sprint_member.discord_user_id}> sent in a final word count of '
                           f'{finish_word_count}, which was less than their starting word count of {start_word_count}. '
                           f'This isn\'t possible! Skipping user for leaderboard calculations.')
            continue

        if finish_word_count == start_word_count:
            await ctx.send(f'Sprint member <@{sprint_member.discord_user_id}> didn\'t type at all...that makes me a '
                           f'sad robot :(')

        word_count = finish_word_count - start_word_count
        submission = Submission(connection=connection, member=sprint_member, word_count=word_count, _type='DELTA')
        submission.create()
        active_sprint.add_submission(submission)
        if sprintathon_is_active:
            active_sprintathon.add_submission(submission)

        sprint_word_counts[sprint_member.discord_user_id] = word_count

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
        global active_sprint
        user_id = ctx.message.author.id
        user_name = ctx.message.author.name
        response = f'{user_name} checked in with {word_count} words!'

        member = Member(connection=connection).find_by_name(user_name)
        if not member:
            member = Member(connection=connection, name=user_name, discord_user_id=user_id)
            member.create()

        active_sprint.add_member(member)

        submission = Submission(connection=connection, member=member, word_count=word_count)
        if submission.member.has_submission_in(active_sprint):
            submission.type = 'FINISH'
        else:
            submission.type = 'START'
        submission.create()

        active_sprint.add_submission(submission)

        if sprintathon_is_active:
            active_sprintathon.add_submission(submission)

        await ctx.send(response)

    bot.run(discord_token)

    connection.close()


if __name__ == '__main__':
    main()
