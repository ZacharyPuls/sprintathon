import asyncio
import logging
import datetime
import math

from discord.ext import commands

from member import Member
from server import Server
from sprint import Sprint
from sprintathon import Sprintathon
from submission import Submission

_debug_mode: bool
_debug_guild: str


def _should_handle_command(ctx):
    return (_debug_mode and ctx.guild.name == _debug_guild) or (not _debug_mode and ctx.guild.name != _debug_guild)


class SprintathonBot(commands.Cog):
    def __init__(self, _bot, connection, debug_mode, debug_guild, _version):
        self.bot = _bot
        self.connection = connection
        self.logger = logging.getLogger('sprintathon.SprintathonBot')
        global _debug_mode
        _debug_mode = debug_mode
        global _debug_guild
        _debug_guild = debug_guild
        self._version = _version

    @commands.Cog.listener()
    async def on_ready(self):
        self.logger.info('Sprintathon is started and ready to handle requests.')
        # Find and handle any orphaned sprints/spr*ntathons
        await self._handle_orphaned_sprintathons()
        await self._handle_orphaned_sprints()

    @commands.command(name='help', brief='Command help', help='Use this command to print this help message.',
                      pass_context=True)
    @commands.check(_should_handle_command)
    async def print_help(self, ctx, *args: str):
        if ctx.message.content.startswith('!'):
            return
        await ctx.send(":robot: Hi, I'm Spr\\*ntathon Bot! Beep boop! :robot:\n"
                       "Here is a list of all the commands I know:\n"
                       "`   Help: Just mention my name and send \"help\", I'll print this message!`\n"
                       "`   !about (or !info): Use this command to get detailed information about me, "
                       "the Spr\\*ntathon Bot!`\n"
                       "`   !start_sprintathon [duration]: Use this command to create (and start) a new "
                       "Spr\\*ntathon, given a duration in hours. Leave the duration blank for a 24hr Spr\\*ntathon.`\n"
                       "`   !stop_sprintathon: Use this command to stop the currently running Spr\\*ntathon, "
                       "if one is running. If there is not a Spr\\*ntathon currently running, this command does "
                       "nothing.`\n"
                       "`   !start_sprint [duration]: Use this command to create (and start) a new Sprint, given a "
                       "duration in minutes. Leave the duration blank for a 15min Sprint.`\n"
                       "`   !stop_sprint: Use this command to stop the currently running Sprint, if one is running. "
                       "If there is not a Sprint currently running, this command does nothing.`\n"
                       "`   !sprint [word_count]: Use this command to check into the currently running Sprint, "
                       "given a word count, or the keyword 'same' to use your previously submitted word count.`\n"
                       "`   !leaderboard: Use this command to print out the current Spr\\*ntathon's leaderboard.`\n"
                       "`   !version: Use this command to print out the current application version.`")

    @print_help.error
    async def print_help_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Ignore errors from _should_handle_command check
            return
        raise error

    @commands.command(name='about', brief='About Spr*ntathon', aliases=['info'],
                      help='Use this command to get detailed information about the Spr*ntathon bot.')
    @commands.check(_should_handle_command)
    async def print_about(self, ctx):
        self.logger.info(f'User {ctx.message.author.name} requested about.')
        await ctx.send(
            ':robot: Hi! I\'m the Spr\\*ntathon bot! Beep boop :robot:\nIt\'s really nice to meet you!\n I\'m so '
            'happy to help my fiancée and her friends track their Sprints! :heart:\n*If you have any questions, '
            'feel free to drop me an email at zach@zachpuls.com, or check out my source code on '
            'https://github.com/ZacharyPuls/sprintathon! If you find any issues, please do create an Issue '
            '(or even a PR) on GitHub, so I can get to fixing it! Thanks again for using Spr\\*ntathon bot!*')

    @print_about.error
    async def print_about_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Ignore errors from _should_handle_command check
            return
        raise error

    @commands.command(name='start_sprintathon', brief='Starts a new Spr*ntathon',
                      help='Use this command to create (and start) a new Spr*ntathon, given a duration in hours. '
                           'Leave the duration blank for a 24hr Spr*ntathon.')
    @commands.check(_should_handle_command)
    async def start_sprintathon(self, ctx, sprintathon_time_in_hours: int = 24):
        if Sprintathon.get_active_for_channel(self.connection, ctx.channel.id) is not None:
            await ctx.send(f'There is already a Spr*ntathon active for this channel! Use `!start_sprint [duration]` '
                           f'to start a new Sprint, or if there is already one active, `!sprint [word_count]` to join '
                           f'the currently running Sprint.')
            return
        self.logger.info('Starting sprintathon with duration of %i hours.', sprintathon_time_in_hours)
        _sprintathon = await self.start_new_sprintathon(ctx, sprintathon_time_in_hours)
        await self.run_sprintathon(_sprintathon)

    @start_sprintathon.error
    async def start_sprintathon_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Ignore errors from _should_handle_command check
            return
        raise error

    @commands.command(name='stop_sprintathon', brief='Stops the current Spr*ntathon',
                      help='Use this command to stop the currently running Spr*ntathon, if one is running. If there '
                           'is not a Spr*ntathon currently running, this command does nothing.')
    @commands.check(_should_handle_command)
    async def stop_sprintathon(self, ctx):
        await self.kill_sprintathon(ctx)

    @stop_sprintathon.error
    async def stop_sprintathon_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Ignore errors from _should_handle_command check
            return
        raise error

    @commands.command(name='start_sprint', brief='Starts a new Sprint',
                      help='Use this command to create (and start) a new Sprint, given a duration in minutes. Leave '
                           'the duration blank for a 15min Sprint.')
    @commands.check(_should_handle_command)
    async def start_sprint(self, ctx, sprint_time_in_minutes: int = 15):
        _server = await self._get_or_create_server(ctx.guild.name, ctx.guild.id)
        if Sprint.get_most_recent_active(self.connection, _server, ctx.channel.id) is not None:
            await ctx.send(f'There is already a Sprint active for this channel! Use `!sprint [word_count]` to join '
                           f'the currently running Sprint.')
            return
        self.logger.info('Starting sprint with duration of %i minutes.', sprint_time_in_minutes)
        _sprint = await self.start_new_sprint(ctx, sprint_time_in_minutes)
        await self.run_sprint(_sprint)

    @start_sprint.error
    async def start_sprint_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Ignore errors from _should_handle_command check
            return
        raise error

    @commands.command(name='stop_sprint', brief='Stops the current Sprint',
                      help='Use this command to stop the currently running Sprint, if one is running. If there is not '
                           'a Sprint currently running, this command does nothing.')
    @commands.check(_should_handle_command)
    async def stop_sprint(self, ctx):
        await self.kill_sprint(ctx)

    @stop_sprint.error
    async def stop_sprint_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Ignore errors from _should_handle_command check
            return
        raise error

    @commands.command(name='sprint', brief='Checks into the current Sprint',
                      help='Use this command to check into the currently running Sprint, given a word count, '
                           'or the keyword \'same\' to use your previously submitted word count.')
    @commands.check(_should_handle_command)
    async def sprint(self, ctx, word_count_str: str):
        user_id = ctx.message.author.id
        user_name = ctx.message.author.name

        member = Member(connection=self.connection).find_by_name(user_name)
        if not member:
            member = Member(connection=self.connection, name=user_name, discord_user_id=user_id)
            member.create()

        _server = await self._get_or_create_server(ctx.guild.name, ctx.guild.id)
        _server.add_member(member)

        if word_count_str.lower() != 'same':
            if not word_count_str.isnumeric():
                await ctx.send(f':four: :zero: :four: Something went wrong. Try again! :four: :zero: :four:')
                return
            word_count = int(word_count_str)
        else:
            member_last_submission = Submission.get_last_for_member(self.connection, member)
            if member_last_submission is not None:
                word_count = member_last_submission.word_count
            else:
                await ctx.send(f'<@{user_id}>, you can\'t use ```!sprint same``` without having a previous submission.')
                return

        self.logger.info('Member %s is checking in with a word_count of %i.', user_name, word_count)
        response = f'{user_name} checked in with {word_count} words!'

        _sprint = Sprint.get_most_recent_active(self.connection, _server, ctx.channel.id)
        if _sprint is None:
            await ctx.send('There isn\'t a Sprint active! Make sure to start one with !start_sprint [duration] before '
                           'submitting your word count. ')
            return
        _sprint.add_member(member)

        submission = Submission(connection=self.connection, member=member, word_count=word_count)
        if submission.member.has_submission_in(_sprint):
            submission.type = 'FINISH'
        else:
            submission.type = 'START'
        submission.create()

        _sprint.add_submission(submission)

        await ctx.send(response)

    @sprint.error
    async def sprint_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Ignore errors from _should_handle_command check
            return
        raise error

    @commands.command(name='leaderboard', brief='Spr*ntathon Leaderboard',
                      help='Use this command to print out the current Spr*ntathon\'s leaderboard.')
    @commands.check(_should_handle_command)
    async def print_leaderboard(self, ctx):
        _sprintathon = Sprintathon.get_active_for_channel(self.connection, ctx.channel.id)
        if not _sprintathon:
            # If no Spr*ntathon is currently active, print the previous Spr*ntathon's leaderboard
            _sprintathon = Sprintathon.get_most_recent_for_channel(self.connection, ctx.channel.id)
        if not _sprintathon:
            await ctx.send(f'No Spr\\*ntathons have been run yet, so I can\'t calculate a leaderboard. Go ahead and '
                           f'start a new Spr\\*ntathon, and check back later!')
        else:
            await self._print_sprintathon_leaderboard(_sprintathon)

    @print_leaderboard.error
    async def print_leaderboard_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Ignore errors from _should_handle_command check
            return
        raise error

    @commands.command(name='version', brief='Show Spr*ntathon version',
                      help='Use this command to print out the current application version.')
    @commands.check(_should_handle_command)
    async def print_version(self, ctx):
        await ctx.send(f'Spr*ntathon application v{self._version} © 2020 Zachary Puls - '
                       f'https://github.com/ZacharyPuls/sprintathon')

    @print_version.error
    async def print_version_error(self, ctx, error):
        if isinstance(error, commands.CheckFailure):
            # Ignore errors from _should_handle_command check
            return
        raise error

    async def run_sprintathon(self, _sprintathon):
        current_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
        sprintathon_start_time = _sprintathon.start.astimezone(datetime.timezone.utc)
        time_elapsed = current_time - sprintathon_start_time
        seconds_to_wait = _sprintathon.duration * 60 * 60 - time_elapsed.total_seconds()
        self.logger.debug(f'[run_sprintathon] Time elapsed: {time_elapsed} | Current time: {current_time} | '
                          f'Sprintathon start time: {sprintathon_start_time} | Seconds to wait: {seconds_to_wait}')

        if seconds_to_wait > 0:
            if _debug_mode:
                await asyncio.sleep(90)
            else:
                if seconds_to_wait > 3600:
                    await asyncio.sleep(seconds_to_wait - 3600)
                    await self.bot.get_channel(_sprintathon.discord_channel_id).send(
                        f':exclamation: :exclamation: :exclamation: We\'re getting close to the finale! Get any last '
                        f'words in before your time is up!! :exclamation: :exclamation: :exclamation:')
                    await asyncio.sleep(3600)
                else:
                    self.logger.debug(f'Zombie Spr*ntathon was revived < 3600 seconds before termination '
                                      f'[{seconds_to_wait}], skipping 1hr warning message.')
                    await asyncio.sleep(seconds_to_wait)

        # If the Spr*ntathon was cancelled by the user while we were sleeping, stop running.
        _sprintathon.fetch()
        if not _sprintathon.active:
            return

        await self.bot.get_channel(_sprintathon.discord_channel_id).send(
            ':clapper: :clapper: :clapper: And cut!! :clapper: :clapper: :clapper:\nThat’s a wrap for this '
            'Spr\\*ntathon! Congratulations to everyone that participated. Let’s see how everyone placed!')
        await self._print_sprintathon_leaderboard(_sprintathon)
        _sprintathon.active = False
        _sprintathon.update()

    async def run_sprint(self, _sprint):
        current_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
        sprint_start_time = _sprint.start.astimezone(datetime.timezone.utc)
        time_elapsed = current_time - sprint_start_time
        self.logger.info(f'Time elapsed: {time_elapsed} | Current time: {current_time} | '
                         f'Sprint start time: {sprint_start_time}')
        seconds_to_wait = _sprint.duration * 60 - time_elapsed.total_seconds()

        if seconds_to_wait > 0:
            if _debug_mode:
                await asyncio.sleep(10)
            else:
                await asyncio.sleep(seconds_to_wait)

        # If the sprint was cancelled by the user while we were sleeping, stop running.
        _sprint.fetch()
        if not _sprint.active:
            return

        await self.bot.get_channel(_sprint.discord_channel_id).send(_sprint.time_is_up_message())

        if _debug_mode:
            await asyncio.sleep(15)
        else:
            await asyncio.sleep(7 * 60)

        # If the sprint was cancelled by the user while we were sleeping, stop running.
        _sprint.fetch()
        if not _sprint.active:
            return

        await self._calculate_and_print_sprint_results(_sprint)

        _sprint.active = False
        _sprint.update()

    async def kill_sprintathon(self, ctx):
        _server = await self._get_or_create_server(ctx.guild.name, ctx.guild.id)
        _sprintathon = Sprintathon.get_active_for_channel(self.connection, ctx.channel.id)

        if _sprintathon is not None:
            _sprintathon.active = False
            _sprintathon.update()
            response = ':x: :x: :x: No problem. Spr\\*ntathon has been cancelled. Maybe next time. :x: :x: :x:'
            self.logger.info('User %s stopped sprintathon.', ctx.message.author.name)
        else:
            response = f':question: <@{ctx.message.author.id}>, there isn\'t an active Spr\\*ntathon for you to stop ' \
                       f':question: '

        await ctx.send(response)

    async def kill_sprint(self, ctx):
        _server = await self._get_or_create_server(ctx.guild.name, ctx.guild.id)
        _sprint = Sprint.get_most_recent_active(self.connection, _server, ctx.channel.id)
        if _sprint is not None:
            _sprint.active = False
            _sprint.update()
            response = ':x: :x: :x: No problem. The current sprint has been cancelled. Maybe next time. :x: :x: :x:'
            self.logger.info('User %s stopped sprint.', ctx.message.author.name)
        else:
            response = f':question: <@{ctx.message.author.id}>, there isn\'t an active Sprint for you to stop :question:'

        await ctx.send(response)

    async def start_new_sprintathon(self, ctx, sprintathon_time_in_hours):
        _server = await self._get_or_create_server(ctx.guild.name, ctx.guild.id)
        _sprintathon = Sprintathon(connection=self.connection, duration=sprintathon_time_in_hours, _server=_server,
                                   discord_channel_id=ctx.channel.id)
        _sprintathon.create()
        hour_or_hours = 'hour'
        if sprintathon_time_in_hours != 1:
            hour_or_hours = 'hours'
        response = f':loudspeaker: :loudspeaker: :loudspeaker: It\'s spr\\*ntathon time! Starting a timer for ' \
                   f'{sprintathon_time_in_hours} {hour_or_hours}. :loudspeaker: :loudspeaker: '
        await ctx.send(response)
        return _sprintathon

    async def start_new_sprint(self, ctx, sprint_time_in_minutes):
        _server = await self._get_or_create_server(ctx.guild.name, ctx.guild.id)
        channel_id = ctx.channel.id
        active_sprintathon = Sprintathon.get_active_for_channel(self.connection, channel_id)
        _sprint = Sprint(connection=self.connection, duration=sprint_time_in_minutes, _server=_server, active=True,
                         _sprintathon=active_sprintathon, discord_channel_id=channel_id)
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

    async def _handle_orphaned_sprintathons(self):
        for _sprintathon in Sprintathon.get_active(self.connection):
            self.logger.warning('Reviving orphaned sprintathon %s.', _sprintathon)
            asyncio.create_task(self.run_sprintathon(_sprintathon))

    async def _handle_orphaned_sprints(self):
        for _sprint in Sprint.get_active(self.connection):
            self.logger.warning('Reviving orphaned sprint %s.', _sprint)
            asyncio.create_task(self.run_sprint(_sprint))

    async def _get_or_create_server(self, guild_name, guild_id):
        return Server(self.connection, name=guild_name, discord_guild_id=guild_id).find_or_create()

    @staticmethod
    def _format_leaderboard_string(leaderboard, leaderboard_wpm):
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

            message += f'    {str(position + 1)}{st_nd_or_th}: <@{result[0]}> - {result[1]} {word_or_words} ' \
                       f'[avg {leaderboard_wpm[result[0]]} wpm]\n'

        return message

    async def _print_sprintathon_leaderboard(self, _sprintathon):
        sprintathon_word_counts = dict()
        sprintathon_wpm = dict()
        for sprintathon_member in _sprintathon.get_members():
            normal_word_count = _sprintathon.get_word_count(sprintathon_member)
            sprintathon_word_counts[
                sprintathon_member.discord_user_id] = normal_word_count + _sprintathon.get_bonus_word_count(
                sprintathon_member)
            sprintathon_wpm[sprintathon_member.discord_user_id] = int(
                math.ceil(normal_word_count / (_sprintathon.duration * 60)))
        sprintathon_leaderboard = sorted(sprintathon_word_counts.items(), key=lambda item: item[1], reverse=True)
        await self.bot.get_channel(_sprintathon.discord_channel_id).send(
            self._format_leaderboard_string(sprintathon_leaderboard, sprintathon_wpm))

    async def _calculate_and_print_sprint_results(self, _sprint):
        channel = self.bot.get_channel(_sprint.discord_channel_id)
        sprint_word_counts = dict()
        for sprint_member in _sprint.get_members():
            self.logger.debug(f'Performing Sprint leaderboard calculation for {sprint_member}')
            submissions = Submission.find_all_by_member_and_sprint(self.connection, sprint_member, _sprint)
            finish_word_count = next((item.word_count for item in submissions if item.type == 'FINISH'), None)
            start_word_count = next((item.word_count for item in submissions if item.type == 'START'), None)
            if start_word_count is None or finish_word_count is None:
                await channel.send(
                    f'Oh no! Sprint member <@{sprint_member.discord_user_id}> forgot to submit their final word '
                    f'count! They will be excluded from this sprint.')
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
            self.logger.info(f'Creating DELTA Submission for {sprint_member.name} with a word_count of {word_count}.')
            submission = Submission(connection=self.connection, member=sprint_member, word_count=word_count,
                                    _type='DELTA')
            submission.create()
            _sprint.add_submission(submission)

            sprint_word_counts[sprint_member.discord_user_id] = word_count
        sprint_leaderboard = sorted(sprint_word_counts.items(), key=lambda item: item[1], reverse=True)
        message = '**Sprint is done! Here are the results:**\n'
        sprint_wpm = {user_id: int(math.ceil(sprint_word_counts[user_id] / _sprint.duration)) for user_id in
                      sprint_word_counts.keys()}
        message += self._format_leaderboard_string(sprint_leaderboard, sprint_wpm)
        await channel.send(message)
        # If there is a sprintathon currently active, award the 1st place member double points.
        if _sprint.sprintathon is not None and _sprint.sprintathon.active and len(sprint_leaderboard) > 0:
            first_place_entry = sprint_leaderboard[0]
            first_place_member = Member(self.connection).find_by_discord_user_id(first_place_entry[0])
            submission = Submission(connection=self.connection, member=first_place_member,
                                    word_count=first_place_entry[1],
                                    _type='BONUS')
            submission.create()
            _sprint.sprintathon.add_submission(submission)
            await channel.send(
                f'**Member <@{first_place_entry[0]}> got first place, so they get double points for the Spr*ntathon!**')
