import subprocess
import bot


# Prints huge text
def command_shout(message):
    channel = message.channel
    message = message.content.lstrip('.')[6:]  # removes the dots and 'shout '
    if len(message) == 0:
        return bot.client.send_message(channel, 'Ai, nã sei gritar isso...')
    result = subprocess.run(['toilet', '-f', 'bigmono9', message], stdout=subprocess.PIPE)
    return bot.client.send_message(channel, '```' + result.stdout.decode('utf-8')[0:2000 - 6] + '```')


# Figures out the caller penis size (super sophisticated algorithm, made by very adult people)
def command_penis(message):
    user_id = int(message.author.id)
    penis_length = user_id % 25
    stick = '=' * penis_length
    if message.author.name == bot.settings['owner']:
        return bot.client.send_message(message.channel, '8============================================D')
    else:
        return bot.client.send_message(message.channel, '8' + stick + 'D')


def populate_commands(commands):
    commands['shout'] = command_shout
    commands['penis'] = command_penis
