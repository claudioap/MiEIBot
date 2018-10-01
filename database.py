import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.declarative import declarative_base


def create_session_factory():
    engine = sa.create_engine('sqlite:///conf/discord.db', echo=False)
    session_factory = sa.orm.sessionmaker(bind=engine)
    return sa.orm.scoped_session(session_factory)


# Models
Base = declarative_base()


class Setting(Base):
    __tablename__ = 'settings'
    name = sa.Column(sa.String(50), primary_key=True)
    value = sa.Column(sa.String(200))


class Student(Base):
    __tablename__ = 'students'
    discord_id = sa.Column(sa.String(50), primary_key=True)
    clip_abbr = sa.Column(sa.String(50), primary_key=True)
    certainty = sa.Column(sa.Integer)
    token = sa.Column(sa.String(10))


class Expression(Base):
    __tablename__ = 'expressions'
    regex = sa.Column(sa.TEXT, primary_key=True)
    message = sa.Column(sa.TEXT)
    embed_name = sa.Column(sa.ForeignKey("embeds.name"))
    embed = orm.relationship("Embed", back_populates="expressions")


class Embed(Base):
    __tablename__ = 'embeds'
    name = sa.Column(sa.String(50), primary_key=True)
    url = sa.Column(sa.TEXT)
    expressions = orm.relationship(Expression, back_populates="embed")
