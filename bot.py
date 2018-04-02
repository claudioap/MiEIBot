import re
import traceback
from datetime import datetime
from time import sleep

import discord
import sqlite3
import logging

client = discord.Client()
settings = {}

logging.basicConfig(level=logging.INFO)

escape = '.'
triggers = {}
commands = {}


def run():
    while True:
        try:
            populate()
            client.run(settings['token'])
        except:
            # FIXME not elegant but will do for now
            print(f'Oops, I crashed...@{datetime.now}\n{traceback.print_exc()}')
            sleep(10)


@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')


@client.event
async def on_member_join(member):
    global client
    await client.send_message(
        client.get_channel(settings['main_ch']),
        (settings['greeting']).format(member.mention))


@client.event
async def on_message(message):
    global client
    answers = 0
    if message.author == client.user:
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
            await client.delete_message(message)
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
    database = sqlite3.connect('./conf/discord.db')
    populate_settings(database)
    populate_parser_data(database)
    database.close()


def populate_settings(database):
    db_cursor = database.cursor()
    db_cursor.execute(
        'SELECT name, value'
        ' FROM Settings')
    global settings
    settings = {row[0]: row[1] for row in db_cursor.fetchall()}


def populate_parser_data(database):
    db_cursor = database.cursor()
    db_cursor.execute(
        'SELECT Expressions.regex, Expressions.message, Embeds.url'
        ' FROM Expressions'
        ' LEFT JOIN Embeds ON Expressions.embed_name = Embeds.name')

    for row in db_cursor.fetchall():
        if row[2] is None and row[1] is not None:
            triggers[re.compile(row[0])] = lambda message, text=row[1]: client.send_message(message.channel, text)
        elif row[2] is not None:
            embed = discord.Embed()
            embed.set_image(url=row[2])
            if row[1] is None:
                triggers[re.compile(row[0])] = lambda message, embed=embed: client.send_message(message.channel,
                                                                                                embed=embed)
            else:
                triggers[re.compile(row[0])] = lambda message, text=row[1], embed=embed: client.send_message(
                    message.channel,
                    text,
                    embed=embed)
        else:
            print("Invalid row: " + str(row))
