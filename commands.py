import subprocess
import bot
from clip import Controller

clip_controller = Controller()

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

def command_clip(message):
    channel = message.channel
    message = message.content.lstrip('.')[5:]  # removes the dots and 'clip '
    if len(message) == 0:
        return bot.client.send_message(channel, 'Nã conheço esse magano...')
    possibilities = clip_controller.whois(message)
    possibilities_str = ""
    for possibility in possibilities:
        possibilities_str += "Numero:{},\tNome:{},\tIdentificador:{}\n".format(
            possibility[0], possibility[1], possibility[2])
    if len(possibilities) == 0:
        return bot.client.send_message(channel, 'Nã conheço esse magano...')
    if len(possibilities) == 1:
        return bot.client.send_message(channel, 'É este gajo:\n' + possibilities_str)
    else:
        return bot.client.send_message(channel, 'Um destes:\n' + possibilities_str)


def populate_commands(commands):
    commands['shout'] = command_shout
    commands['penis'] = command_penis
    commands['clip'] = command_clip