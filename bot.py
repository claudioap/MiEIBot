import re
import discord
import sqlite3
import logging

from commands import populate_commands

logging.basicConfig(level=logging.INFO)

escape = '.'
triggers = {}
commands = {}

global client
client = discord.Client()
global settings
settings = {}


def run():
    populate()
    client.run(settings['token'])


@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')


@client.event
async def on_member_join(member):
    await client.send_message(
        client.get_channel(settings['main_ch']),
        (settings['greeting']).format(member.mention))


@client.event
async def on_message(message):
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
