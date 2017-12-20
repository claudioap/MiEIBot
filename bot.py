import re
import discord
import sqlite3

escape = '.'
bot_id = None
settings = {}
triggers = {}
commands = {}

client = discord.Client()


def run():
    load_db()
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
    if message.author.name == 'MiEIBot':
        return

    if message.content.startswith(escape):
        pass  # TODO for later
    else:
        for expression in triggers:
            if expression.search(message.content):
                answers += 1
                await triggers[expression](message)
                if answers >= 2:
                    break


def load_db():
    database = sqlite3.connect('discord.db')
    populate_settings(database)
    populate_parser_data(database)
    database.close()


def populate_settings(database):
    db_cursor = database.cursor()
    db_cursor.execute(
        'SELECT name, value'
        ' FROM Settings')
    for row in db_cursor.fetchall():
        settings[row[0]] = row[1]


def populate_parser_data(database):
    db_cursor = database.cursor()
    db_cursor.execute(
        'SELECT Expressions.regex, Expressions.message, Embeds.url'
        ' FROM Expressions'
        ' LEFT JOIN Embeds ON Expressions.embed_name = Embeds.name')

    for row in db_cursor.fetchall():
        if row[2] is None and row[1] is not None:
            triggers[re.compile(row[0])] = lambda message, text=row[1]: client.send_message(message.channel, text)
        elif row[1] is None and row[2] is not None:
            embed = discord.Embed()
            embed.set_image(url=row[2])
            triggers[re.compile(row[0])] = lambda message, embed=embed: client.send_message(message.channel,
                                                                                            embed=embed)
        elif row[1] is not None and row[2] is not None:
            embed = discord.Embed()
            embed.set_image(url=row[2])
            triggers[re.compile(row[0])] = lambda message, text=row[1], embed=embed: client.send_message(
                message.channel,
                text,
                embed=embed)
        else:
            print("Invalid row: " + str(row))
