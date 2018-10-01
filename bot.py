import re
import traceback
from datetime import datetime
from time import sleep

import discord
import logging

from sqlalchemy.orm import Session

import database as db

logging.basicConfig(level=logging.INFO)

bot = discord.Client()
settings = {}
DBSession = db.create_session_factory()

escape = '.'
triggers = {}
commands = {}


def run():
    while True:
        try:
            populate()
            bot.run(settings['token'])
        except:
            # FIXME not elegant but will do for now
            print(f'Oops, I crashed...@{datetime.now}\n{traceback.print_exc()}')
            sleep(10)


@bot.event
async def on_ready():
    print('Logged in as')
    print(bot.user.name)
    print(bot.user.id)
    print('------')


@bot.event
async def on_member_join(member):
    global bot
    await bot.send_message(
        bot.get_channel(settings['main_ch']),
        (settings['greeting']).format(member.mention))


@bot.event
async def on_message(message):
    global bot
    answers = 0
    if message.author == bot.user:
        return

    if message.content.startswith(escape):
        command = message.content.split()[0]
        if len(command) < 2:
            return
        delete_parent = False
        if command[1] == '.':
            delete_parent = True
        command = command.lstrip('.')
        if command in commands:
            command = commands[command]
            await command(message)
        else:
            print("Unknown command: " + command)

        if delete_parent:
            await bot.delete_message(message)
    else:
        for expression in triggers:
            if expression.search(message.content):
                answers += 1
                await triggers[expression](message)
                if answers >= 2:
                    break


def populate():
    load_db()
    from commands import populate_commands  # TODO more elegant solution
    populate_commands(commands)


def load_db():
    db_session: Session = DBSession()
    populate_settings(db_session)
    populate_parser_data(db_session)
    DBSession.remove()


def populate_settings(db_session: Session):
    global settings
    settings = {setting.name: setting.value for setting in db_session.query(db.Setting)}


def populate_parser_data(db_session: Session):
    for expression in db_session.query(db.Expression):
        if expression.embed is None and expression.message is not None:
            triggers[re.compile(expression.regex)] = \
                lambda message, text=expression.message: \
                    bot.send_message(message.channel, text)
        elif expression.embed is not None:
            embed = discord.Embed()
            embed.set_image(url=expression.embed.url)
            if expression.message is None:
                triggers[re.compile(expression.regex)] = \
                    lambda message, embed=embed: bot.send_message(message.channel, embed=embed)
            else:
                triggers[re.compile(expression.regex)] = \
                    lambda message, text=expression.message, embed=embed: \
                        bot.send_message(message.channel, text, embed=embed)
        else:
            print(f"Invalid row: {expression}")
