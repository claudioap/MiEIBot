import subprocess
from bot import client, settings
from clip import Controller

clip_controller = Controller()
settings['course'] = clip_controller.find_course('MIEI')


# Prints huge text
def populate_commands(commands):
    commands['shout'] = command_shout
    commands['penis'] = command_penis
    commands['clip'] = command_clip


def command_shout(message):
    channel = message.channel
    message = message.content.lstrip('.')[6:]  # removes the dots and 'shout '
    if len(message) == 0:
        return client.send_message(channel, 'Ai, nã sei gritar isso...')
    result = subprocess.run(['toilet', '-f', 'bigmono9', message], stdout=subprocess.PIPE)
    return client.send_message(channel, '```' + result.stdout.decode('utf-8')[0:2000 - 6] + '```')


# Figures out the caller penis size (super sophisticated algorithm, made by very adult people)
def command_penis(message):
    user_id = int(message.author.id)
    penis_length = user_id % 25
    stick = '=' * penis_length
    if message.author.name == settings['owner']:
        return client.send_message(message.channel, '8============================================D')
    else:
        return client.send_message(message.channel, '8' + stick + 'D')


def command_clip(message):
    channel = message.channel
    message = message.content.lstrip('.')[5:]  # removes the dots and 'clip '
    course = settings['course']
    response = "Á procura de alunos pelo nome `{}`\nFiltro:`{}`\n...\n".format(message, course)
    if len(message) == 0:
        return client.send_message(channel, 'Nã conheço esse magano...')
    possibilities = clip_controller.find_student(message, course_filter=course)
    possibilities_str = ""
    for student in possibilities:
        possibilities_str += str(student) + '\n'
    if len(possibilities) == 0:
        response += 'Nã conheço esse magano...'
    if len(possibilities) == 1:
        response += 'É este gajo:\n```'
    else:
        response += 'Um destes:\n```'
    response += possibilities_str + '```'
    return client.send_message(channel, response)
