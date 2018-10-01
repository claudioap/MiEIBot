import random
import re
import smtplib
import string
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import discord
from sqlalchemy.orm import Session

from bot import bot, settings, DBSession
import database as db

from CLIPy import CacheStorage, Clip

storage = CacheStorage.postgresql(settings['clipy_uname'], settings['clipy_pwd'], settings['clipy_db'])
clip = Clip(storage)

settings['course'] = clip.find_course('MIEI', 2018)


# Prints huge text
def populate_commands(commands):
    commands['shout'] = command_shout
    commands['penis'] = command_penis
    commands['clip'] = command_clip
    commands['sou'] = command_request_validation
    commands['validar'] = command_validate


def command_shout(message):
    channel = message.channel
    message = message.content.lstrip('.')[6:]  # removes the dots and 'shout '
    if len(message) == 0:
        return bot.send_message(channel, 'Ai, nã sei gritar isso...')
    result = subprocess.run(['toilet', '-f', 'bigmono9', message], stdout=subprocess.PIPE)
    return bot.send_message(channel, '```' + result.stdout.decode('utf-8')[0:2000 - 6] + '```')


# Figures out the caller penis size (super sophisticated algorithm, made by very adult people)
def command_penis(message):
    user_id = int(message.author.id)
    penis_length = user_id % 25
    stick = '=' * penis_length
    if message.author.name == settings['owner']:
        return bot.send_message(message.channel, '8============================================D')
    else:
        return bot.send_message(message.channel, '8' + stick + 'D')


def command_request_validation(message):
    channel = message.channel
    clip_abbr = message.content.lstrip('.')[4:]  # removes the dots and 'sou '
    if not valid_clip_nick(clip_abbr):
        return bot.send_message(channel, "Não me parece que seja esse o teu nick do clip...")
    author = str(message.author)
    session: Session = DBSession()
    user_validation = session.query(db.Student).filter_by(discord_id=author).first()
    print(f'Asked to validate {author} as {clip_abbr}')
    try:
        if user_validation is None:
            if session.query(db.Student).filter_by(clip_abbr=clip_abbr).first() is not None:
                return bot.send_message(channel, "Esse estudante já foi/tentou ser registado.")

            token = generate_token(8)
            send_mail(email=f"{clip_abbr}@campus.fct.unl.pt", token=token)
            session.add(db.Student(token=token, discord_id=author, clip_abbr=clip_abbr, certainty=0))
            session.commit()
            return bot.send_message(channel,
                                    f"Enviado email de confirmação para `{clip_abbr}@campus.fct.unl.pt`\n"
                                    "Responde `.validar [código]` a esta mensagem")
        else:
            return bot.send_message(channel, "Já se encontra uma validação em progresso para ti.")
    finally:
        DBSession.remove()


async def command_validate(message):
    channel = message.channel
    token = message.content.lstrip('.')[8:]  # removes the dots and 'valida '
    author = message.author
    if not hasattr(message.author, 'server'):
        return bot.send_message(channel, "Manda-me uma mensagem no servidor não por PM.")
    role = discord.utils.get(message.author.server.roles, name="Verificado")
    if role is None:
        return bot.send_message(channel, "Não encontrei a role certa para ti. Alguém mexeu onde não devia!\n"
                                         "Lamento mas vais ter de esperar que um dos mods veja isto.\n"
                                         "Podes mandar-lhes mensagem.")
    await bot.add_roles(author, role)
    session: Session = DBSession()
    student_validation: db.Student = session.query(db.Student).filter_by(discord_id=str(author)).first()
    print(f'Asked to validate {author} with token {token}')
    try:
        if student_validation is None:
            return bot.send_message(channel, "Não me lembro de ti...")
        elif student_validation.token == token:
            student_validation.certainty = 1
            session.commit()
            return bot.send_message(channel, "Validado com sucesso.")
        else:
            return bot.send_message(channel, "Token incorreto.")
    finally:
        DBSession.remove()


def command_clip(message):
    channel = message.channel
    message = message.content.lstrip('.')[5:]  # removes the dots and 'clip '
    course = settings['course']
    response = "Á procura de alunos pelo nome `{}`\nFiltro:`{}`\n...\n".format(message, course)
    if len(message) == 0:
        return bot.send_message(channel, 'Nã conheço esse magano...')
    possibilities = clip.find_student(message, course_filter=course)
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
    return bot.send_message(channel, response)


def generate_token(length: int) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def send_mail(email, token):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Validação discord MiEI"
    msg['From'] = settings['email_user']
    msg['To'] = email

    text = f"O teu token para o discord de MiEI é {token}. Responde ao bot '.validar {token}'"
    msg.attach(MIMEText(text, 'plain'))
    server = smtplib.SMTP(settings['email_server'], 587, 30)
    server.ehlo()
    server.starttls()
    server.login(settings['email_user'], settings['email_password'])
    server.sendmail(settings['email_user'], email, msg.as_string())
    server.quit()


def valid_clip_nick(clip_abbr) -> bool:
    if len(clip_abbr) < 5:
        return False
    exp = re.compile('^[a-z.]*$')
    if exp.fullmatch(clip_abbr) is None:
        return False
    return True
