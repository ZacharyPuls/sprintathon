import os
import logging
import re
import datetime

from discord.ext import commands
from dotenv import load_dotenv
import asyncio
import psycopg2

from member import Member
from submission import Submission
from sprint import Sprint
from sprintathon import Sprintathon
import server

connection = None

debug_mode_enabled: bool

__version__ = [1, 0, 2]
migrations_directory = 'db/migrations'


def add_sprintathon_submission(sprintathon_id, submission_id):
    with connection.cursor() as cursor:
        cursor.execute('INSERT INTO SPRINTATHON_SUBMISSION(SPRINTATHON_ID, SUBMISSION_ID) VALUES(%s, %s)',
                       (sprintathon_id, submission_id))
        connection.commit()


def is_version_string_greater(a, b):
    a_version_numbers = list(map(int, a.split('.')))
    b_version_numbers = list(map(int, b.split('.')))

    if len(b_version_numbers) > len(a_version_numbers):
        compare_from = b_version_numbers
        compare_to = a_version_numbers
        compare = lambda x, y: y > x
    else:
        compare_from = a_version_numbers
        compare_to = b_version_numbers
        compare = lambda x, y: x > y

    for index, version_number in enumerate(compare_from):
        if index < len(compare_to) and version_number != compare_to[index]:
            return compare(version_number, compare_to[index])
        elif index >= len(compare_to):
            return version_number != 0

    return False


def initialize_database(connection_uri):
    global connection
    connection = psycopg2.connect(connection_uri)

    with open('db/schema.sql') as schema_file:
        schema = schema_file.read()

    with connection.cursor() as cursor:
        cursor.execute(schema)
        connection.commit()
        cursor.execute('SELECT * FROM VERSION')
        schema_version_result = cursor.fetchone()
        if schema_version_result is None:
            schema_is_blank = True
            schema_version_result = [0, 0, 0]
        schema_version_major = schema_version_result[0]
        schema_version_minor = schema_version_result[1]
        schema_version_patch = schema_version_result[2]

        logger = logging.getLogger('sprintathon.initialize_database')

        target_version = f'{__version__[0]}.{__version__[1]}.{__version__[2]}'
        target_version_dashed = target_version.replace('.', '-')

        schema_version = f'{schema_version_major}.{schema_version_minor}.{schema_version_patch}'
        schema_version_dashed = schema_version.replace('.', '-')

        if is_version_string_greater(schema_version, target_version):
            logger.warning(f'Attempting to run application v{target_version} on database schema v{schema_version}, '
                           f'which is greater. Aborting migration.')
            return False

        if schema_version == target_version:
            logger.info(f'Schema version v{schema_version} is identical to application version v{target_version}. '
                        f'No migrations to apply.')
            return True

        if schema_is_blank:
            logger.info(f'Migrating database schema from blank to v{target_version}.')
        else:
            logger.info(f'Migrating database schema from v{schema_version} to v{target_version}.')

        migration_files = sorted(os.listdir(migrations_directory))
        try:
            if schema_is_blank:
                migration_file_list_start = 0
            else:
                migration_file_list_start = migration_files.index(f'patch_{schema_version_dashed}.sql') + 1
            if migration_file_list_start >= len(migration_files):
                logger.info(f'No migrations to apply.')
                return True
                # I'm not sure I really want to make a migration for each version released, even if it is just to
                # update _VERSION logger.error(f'Migration of database schema with version {schema_version} to {
                # target_version} failed. Application version {target_version} is greater than database schema
                # version {schema_version}, but no migration files are present for any of the versions after {
                # schema_version}.')
            for migration_filename in migration_files[migration_file_list_start:]:
                migration_filename_regex = re.search(r'patch_((\d+-){2}\d+)\.sql', migration_filename)
                if not migration_filename_regex:
                    logger.warning(f'Skipping invalid migration filename {migration_filename}.')
                    continue
                migration_version = migration_filename_regex.group(1).replace('-', '.')
                if is_version_string_greater(migration_version, target_version):
                    logger.info(f'Migration filename {migration_filename} (v{migration_version}) is greater than '
                                f'target version {target_version}. Skipping migration file {migration_filename}.')
                    continue
                try:
                    with open(f'{migrations_directory}/{migration_filename}') as migration_file:
                        migration_file_sql = migration_file.read()
                        if len(migration_file_sql) > 0:
                            cursor.execute(migration_file_sql)
                        else:
                            logger.warning(f'Skipping empty migration file {migration_filename}.')
                except (OSError, psycopg2.DataError) as e:
                    logger.error(f'Failed to apply migration {migration_filename}, exception thrown was: {repr(e)}.')
                    logger.info(f'Rolling back migrations, reverting back to v{schema_version}.')
                    connection.rollback()
                    return False
        except ValueError as e:
            logger.error(f'Migration of database schema with version '
                         f'{schema_version} to {target_version} '
                         f'failed. Could not find a file in {migrations_directory} named '
                         f'patch_{target_version_dashed}.sql. [{repr(e)}]')
            return False

    logger.info(f'Migration from v{schema_version} to v{target_version} was successful, committing transaction.')
    connection.commit()

    return True


async def run_sprintathon(bot, _sprintathon):
    current_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
    sprintathon_start_time = _sprintathon.start.astimezone(datetime.timezone.utc)
    time_elapsed = current_time - sprintathon_start_time
    seconds_to_wait = _sprintathon.duration * 60 * 60 - time_elapsed.total_seconds()
    logging.getLogger('sprintathon.main.run_sprintathon').debug(
        f'Time elapsed: {time_elapsed} | Current time: {current_time} '
        f'| Sprintathon start time: {sprintathon_start_time} '
        f'| Seconds to wait: {seconds_to_wait}')

    if seconds_to_wait > 0:
        if debug_mode_enabled:
            await asyncio.sleep(90)
        else:
            await asyncio.sleep(seconds_to_wait)

    # If the Spr*ntathon was cancelled by the user while we were sleeping, stop running.
    _sprintathon.fetch()
    if not _sprintathon.active:
        return

    await bot.get_channel(_sprintathon.discord_channel_id).send('**Sprintathon is done! Here are the results:**')
    await print_sprintathon_leaderboard(bot, _sprintathon)
    _sprintathon.active = False
    _sprintathon.update()


async def start_new_sprintathon(ctx, sprintathon_time_in_hours):
    _server = await get_or_create_server(ctx.guild.name, ctx.guild.id)
    _sprintathon = Sprintathon(connection=connection, duration=sprintathon_time_in_hours, _server=_server,
                               discord_channel_id=ctx.channel.id)
    _sprintathon.create()
    hour_or_hours = 'hour'
    if sprintathon_time_in_hours != 1:
        hour_or_hours = 'hours'
    response = f':loudspeaker: :loudspeaker: :loudspeaker: It\'s spr*ntathon time! Starting a timer for ' \
               f'{sprintathon_time_in_hours} {hour_or_hours}. :loudspeaker: :loudspeaker: :loudspeaker: '
    await ctx.send(response)
    return _sprintathon


async def print_sprintathon_leaderboard(bot, _sprintathon):
    sprintathon_word_counts = dict()
    for sprintathon_member in _sprintathon.get_members():
        sprintathon_word_counts[sprintathon_member.discord_user_id] = _sprintathon.get_word_count(sprintathon_member)
    sprintathon_leaderboard = sorted(sprintathon_word_counts.items(), key=lambda item: item[1], reverse=True)
    await bot.get_channel(_sprintathon.discord_channel_id).send(
        format_leaderboard_string(sprintathon_leaderboard, _sprintathon.duration * 60))


async def kill_sprintathon(ctx):
    global sprintathon_is_active
    global active_sprintathon

    active_sprintathon.delete()

    sprintathon_is_active = False
    active_sprintathon = None

    response = ':x: :x: :x: You’re valid. Spr*ntathon has been cancelled. Maybe next time. :x: :x: :x:'

    await ctx.send(response)


async def run_sprint(bot, _sprint):
    current_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
    sprint_start_time = _sprint.start.astimezone(datetime.timezone.utc)
    time_elapsed = current_time - sprint_start_time
    logging.getLogger('sprintathon.main.run_sprint').info(
        f'Time elapsed: {time_elapsed} | Current time: ''{current_time} | ' f'Sprint start time: {sprint_start_time}')
    seconds_to_wait = _sprint.duration * 60 - time_elapsed.total_seconds()

    if seconds_to_wait > 0:
        if debug_mode_enabled:
            await asyncio.sleep(10)
        else:
            await asyncio.sleep(seconds_to_wait)

    # If the sprint was cancelled by the user while we were sleeping, stop running.
    _sprint.fetch()
    if not _sprint.active:
        return

    await bot.get_channel(_sprint.discord_channel_id).send(_sprint.time_is_up_message())

    if debug_mode_enabled:
        await asyncio.sleep(15)
    else:
        await asyncio.sleep(2 * 60)

    # If the sprint was cancelled by the user while we were sleeping, stop running.
    _sprint.fetch()
    if not _sprint.active:
        return

    await calculate_and_print_sprint_results(bot, _sprint)

    _sprint.active = False
    _sprint.update()


async def start_new_sprint(ctx, sprint_time_in_minutes):
    _server = await get_or_create_server(ctx.guild.name, ctx.guild.id)
    channel_id = ctx.channel.id
    _sprint = Sprint(connection=connection, duration=sprint_time_in_minutes, _server=_server, active=True,
                     _sprintathon=Sprintathon.get_active_for_channel(connection, channel_id),
                     discord_channel_id=channel_id)
    _sprint.create()
    minute_or_minutes = 'minute'

    if sprint_time_in_minutes != 1:
        minute_or_minutes = 'minutes'

    response = f'It\'s sprint time, let\'s get typing! Everyone use \'!sprint [word count]\' with your current ' \
               f'word count to check in. I\'m setting the timer for {sprint_time_in_minutes} {minute_or_minutes}, ' \
               f'try and write as much as you can in the allotted time. When the time is up, I will let you know, ' \
               f'and you will have 5 minutes to check in again with your word count.'

    # TODO: allow members to join for 5 minutes before actually starting the sprint

    await ctx.send(response)
    return _sprint


async def calculate_and_print_sprint_results(bot, _sprint):
    channel = bot.get_channel(_sprint.discord_channel_id)
    await channel.send('Word count submission time is up!')
    sprint_word_counts = dict()
    for sprint_member in _sprint.get_members():
        submissions = Submission.find_all_by_member_and_sprint(connection, sprint_member, _sprint)
        finish_word_count = next((item.word_count for item in submissions if item.type == 'FINISH'), None)
        start_word_count = next((item.word_count for item in submissions if item.type == 'START'), None)
        if not start_word_count or not finish_word_count:
            await channel.send(f'Sprint member <@{sprint_member.discord_user_id}> forgot to submit their final word '
                               f'count! Skipping member for leaderboard calculations.')
            continue

        if finish_word_count < start_word_count:
            await channel.send(f'Sprint member <@{sprint_member.discord_user_id}> sent in a final word count of '
                               f'{finish_word_count}, which was less than their starting word count of '
                               f'{start_word_count}. This isn\'t possible! Skipping member for leaderboard '
                               f'calculations.')
            continue

        if finish_word_count == start_word_count:
            await channel.send(f'Sprint member <@{sprint_member.discord_user_id}> didn\'t type at all...'
                               f'that makes me a sad robot :(')

        word_count = finish_word_count - start_word_count
        submission = Submission(connection=connection, member=sprint_member, word_count=word_count, _type='DELTA')
        submission.create()
        _sprint.add_submission(submission)

        sprint_word_counts[sprint_member.discord_user_id] = word_count
    sprint_leaderboard = sorted(sprint_word_counts.items(), key=lambda item: item[1], reverse=True)
    message = '**Sprint is done! Here are the results:**\n'
    message += format_leaderboard_string(sprint_leaderboard, _sprint.duration)
    await channel.send(message)
    # If there is a sprintathon currently active, award the 1st place member double points.
    if _sprint.sprintathon is not None and _sprint.sprintathon.active and len(sprint_leaderboard) > 0:
        first_place_entry = sprint_leaderboard[0]
        first_place_member = Member(connection).find_by_discord_user_id(first_place_entry[0])
        submission = Submission(connection=connection, member=first_place_member, word_count=first_place_entry[1],
                                _type='DELTA')
        submission.create()
        _sprint.sprintathon.add_submission(submission)
        await channel.send(
            f'**Member <@{first_place_entry[0]}> got first place, so they get double points for the Spr*ntathon!**')


async def kill_sprint(ctx):
    _sprint = Sprint(connection).get_most_recent_active(connection, get_or_create_server(ctx.guild.name, ctx.guild.id),
                                                        ctx.channel.id)
    _sprint.delete()

    response = ':x: :x: :x: You’re valid. Sprint has been cancelled. Maybe next time. :x: :x: :x:'

    await ctx.send(response)


def format_leaderboard_string(leaderboard, duration_in_minutes):
    if len(leaderboard) == 0:
        return 'No one joined this round!'
    message = ''
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

        wpm = float(result[1]) / float(duration_in_minutes)
        message += f'    {str(position + 1)}{st_nd_or_th}: <@{result[0]}> - {result[1]} {word_or_words} ' \
                   f'[avg {wpm} wpm]\n'

    return message


def detect_debug_guild_conflict(current_guild, debug_guild, command_name, user_name, logger):
    if debug_mode_enabled and current_guild != debug_guild:
        logger.info(f'Ignoring {command_name} command from {user_name}, debug mode is active, and they attempted to '
                    f'call from the {current_guild} guild.')
        return True
    elif not debug_mode_enabled and current_guild == debug_guild:
        logger.info(f'Ignoring {command_name} command from {user_name}, debug mode is inactive, and they attempted '
                    f'to call from the {current_guild} guild.')
        return True
    return False


async def get_or_create_server(guild_name, guild_id):
    return server.Server(connection, name=guild_name, discord_guild_id=guild_id).find_or_create()


async def handle_orphaned_sprintathons(bot):
    for _sprintathon in Sprintathon.get_active(connection):
        await bot.get_channel(_sprintathon.discord_channel_id).send(
            f'*Oops! Looks like the Spr\\*ntathon bot crashed while processing a Spr\\*ntathon earlier! To make sure '
            f'the Spr\\*ntathon bot follows the ACID principle, I\'m going to pick up where the crashed bot left off, '
            f'and continue the Spr\\*ntathon! Keep in mind, any WPM calculations might be off, depending on how '
            f'quickly the crashed process was restarted. If anything seems off, please shoot a message over to Zach '
            f'Puls, or an email to zach@zachpuls.com.*')
        await run_sprintathon(bot, _sprintathon)


async def handle_orphaned_sprints(bot):
    for _sprint in Sprint.get_active(connection):
        await bot.get_channel(_sprint.discord_channel_id).send(
            f'*Oops! Looks like the Spr\\*ntathon bot crashed while processing a Sprint earlier! To make sure the '
            f'Spr\\*ntathon bot follows the ACID principle, I\'m going to pick up where the crashed bot left off, '
            f'and continue the Sprint! Keep in mind, any WPM calculations might be off, depending on how quickly the '
            f'crashed process was restarted. If anything seems off, please shoot a message over to Zach Puls, '
            f'or an email to zach@zachpuls.com.*')
        await run_sprint(bot, _sprint)


def main():
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('sprintathon')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.info('Starting up sprintathon.')

    load_dotenv()
    logger.info('Loaded environment variables from .env.')
    discord_token = os.environ.get('SPRINTATHON_DISCORD_TOKEN')

    bot = commands.Bot(command_prefix='!')

    connection_string = os.environ.get('SPRINTATHON_PGSQL_CONNECTION_STRING')
    if not initialize_database(connection_string):
        connection.close()
        logger.critical('Failed to initialize database, exiting...')
        return
    logger.info('Connected to database.')

    global debug_mode_enabled
    debug_mode_enabled = os.environ.get('SPRINTATHON_DEBUG_MODE') == 'True'
    enabled_or_disabled = 'disabled'
    if debug_mode_enabled:
        enabled_or_disabled = 'enabled'
    logger.info('Debug mode is %s.', enabled_or_disabled)

    debug_guild = os.environ.get('SPRINTATHON_DEBUG_GUILD')

    @bot.event
    async def on_ready():
        logger.info('Sprintathon is started and ready to handle requests.')
        # Find and handle any orphaned sprints/spr*ntathons
        await handle_orphaned_sprintathons(bot)
        await handle_orphaned_sprints(bot)

    @bot.command(name='about', brief='About Spr*ntathon',
                 help='Use this command to get detailed information about the Spr*ntathon bot.')
    async def print_about(ctx):
        if detect_debug_guild_conflict(ctx.message.guild.name, debug_guild, '!about', ctx.message.author.name, logger):
            return
        logger.info(f'User {ctx.message.author.name} requested about.')
        await ctx.send(
            ':robot: Hi! I\'m the Spr\\*ntathon bot! Beep boop :robot:\nIt\'s really nice to meet you!\n I\'m so '
            'happy to help my fiancée and her friends track their Sprints! :heart:\n*If you have any questions, '
            'feel free to drop me an email at zach@zachpuls.com, or check out my source code on '
            'https://github.com/ZacharyPuls/sprintathon! If you find any issues, please do create an Issue '
            '(or even a PR) on GitHub, so I can get to fixing it! Thanks again for using Spr\\*ntathon bot!*')

    @bot.command(name='start_sprintathon', brief='Starts a new Spr*ntathon',
                 help='Use this command to create (and start) a new Spr*ntathon, given a duration in hours. Leave the '
                      'duration blank for a 24hr Spr*ntathon.')
    async def start_sprintathon(ctx, sprintathon_time_in_hours: int = 24):
        if detect_debug_guild_conflict(ctx.message.guild.name, debug_guild, '!start_sprintathon',
                                       ctx.message.author.name, logger):
            return
        logger.info('Starting sprintathon with duration of %i hours.', sprintathon_time_in_hours)
        _sprintathon = await start_new_sprintathon(ctx, sprintathon_time_in_hours)
        await run_sprintathon(bot, _sprintathon)

    @bot.command(name='stop_sprintathon', brief='Stops the current Spr*ntathon',
                 help='Use this command to stop the currently running Spr*ntathon, if one is running. If there is not '
                      'a Spr*ntathon currently running, this command does nothing.')
    async def stop_sprintathon(ctx):
        if detect_debug_guild_conflict(ctx.message.guild.name, debug_guild, '!stop_sprintathon',
                                       ctx.message.author.name, logger):
            return
        logger.info('User %s stopped sprintathon.', ctx.message.author.name)
        await kill_sprintathon(ctx)

    @bot.command(name='start_sprint', brief='Starts a new Sprint',
                 help='Use this command to create (and start) a new Sprint, given a duration in minutes. Leave the '
                      'duration blank for a 15min Sprint.')
    async def start_sprint(ctx, sprint_time_in_minutes: int = 15):
        if detect_debug_guild_conflict(ctx.message.guild.name, debug_guild, '!start_sprint', ctx.message.author.name,
                                       logger):
            return
        logger.info('Starting sprint with duration of %i minutes.', sprint_time_in_minutes)
        _sprint = await start_new_sprint(ctx, sprint_time_in_minutes)
        await run_sprint(bot, _sprint)

    @bot.command(name='stop_sprint', brief='Stops the current Sprint',
                 help='Use this command to stop the currently running Sprint, if one is running. If there is not a '
                      'Sprint currently running, this command does nothing.')
    async def stop_sprint(ctx):
        if detect_debug_guild_conflict(ctx.message.guild.name, debug_guild, '!stop_sprint', ctx.message.author.name,
                                       logger):
            return
        logger.info('Stopping sprint.')
        await kill_sprint(ctx)

    @bot.command(name='sprint', brief='Checks into the current Sprint',
                 help='Use this command to check into the currently running Sprint, given a word count, '
                      'or the keyword \'same\' to use your previously submitted word count.')
    async def sprint(ctx, word_count_str: str):
        if detect_debug_guild_conflict(ctx.message.guild.name, debug_guild, '!sprint', ctx.message.author.name, logger):
            return
        user_id = ctx.message.author.id
        user_name = ctx.message.author.name

        member = Member(connection=connection).find_by_name(user_name)
        if not member:
            member = Member(connection=connection, name=user_name, discord_user_id=user_id)
            member.create()

        _server = await get_or_create_server(ctx.guild.name, ctx.guild.id)
        _server.add_member(member)

        if word_count_str.lower() != 'same':
            word_count = int(word_count_str)
        else:
            member_last_submission = Submission.get_last_for_member(connection, member)
            if member_last_submission is not None:
                word_count = member_last_submission.word_count
            else:
                await ctx.send(f'<@{user_id}>, you can\'t use ```!sprint same``` without having a previous submission.')
                return

        logger.info('Member %s is checking in with a word_count of %i.', user_name, word_count)
        response = f'{user_name} checked in with {word_count} words!'

        _sprint = Sprint.get_most_recent_active(connection, _server, ctx.channel.id)
        _sprint.add_member(member)

        submission = Submission(connection=connection, member=member, word_count=word_count)
        if submission.member.has_submission_in(_sprint):
            submission.type = 'FINISH'
        else:
            submission.type = 'START'
        submission.create()

        _sprint.add_submission(submission)

        await ctx.send(response)

    @bot.command(name='leaderboard', brief='Spr*ntathon Leaderboard',
                 help='Use this command to print out the current Spr*ntathon\'s leaderboard.')
    async def print_leaderboard(ctx):
        if debug_mode_enabled and ctx.guild.name != debug_guild:
            logger.info(f'Ignoring !leaderboard command from {ctx.message.author.name}, debug mode is active, and '
                        f'they attempted to call from the {ctx.message.guild.name} guild.')
            return
        _sprintathon = Sprintathon.get_active_for_channel(connection, ctx.channel.id)
        await print_sprintathon_leaderboard(bot, _sprintathon)

    bot.run(discord_token)

    logger.info('Shutting down sprintathon.')

    connection.close()
    logger.info('Disconnected from database.')

    logger.info('Sprintathon terminated.')


if __name__ == '__main__':
    main()
