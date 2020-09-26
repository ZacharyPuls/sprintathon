import logging
import os
import re

import psycopg2
from dotenv import load_dotenv
from discord.ext import commands
from sprintathonbot import SprintathonBot

connection = None

debug_mode_enabled: bool

__version__ = [1, 0, 3]
migrations_directory = 'db/migrations'


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
        else:
            schema_is_blank = False
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

    bot = commands.Bot(command_prefix=commands.when_mentioned_or('!'), help_command=None)
    bot.add_cog(SprintathonBot(bot, connection, debug_mode_enabled, debug_guild,
                               f'{__version__[0]}.{__version__[1]}.{__version__[2]}'))

    bot.run(discord_token)

    logger.info('Shutting down sprintathon.')

    connection.close()
    logger.info('Disconnected from database.')

    logger.info('Sprintathon terminated.')


if __name__ == '__main__':
    main()
