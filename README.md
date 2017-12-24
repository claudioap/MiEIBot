# MiEI Bot
We, the students of Computer Science (MiEI) at the New University of Lisbon (Nova Lisboa) united to create a Discord bot, just for fun. We'll eventually teach him some useful tricks.

It was made using [Discord.py](https://github.com/Rapptz/discord.py), and in order to stay friendly to our freshmen we promise not to do weird voodoo magic with this code.

Since we all prefer to keep our private jokes uhm... private!, most trigger expressions and responses are kept local in an SQLite database. We'll keep it to ourselves.

These are the required tables for it to work:
### Settings
| name | value |
|--|--|
| token | [token] |
| main_ch | [channel ID] |
| greeting | [greeting message] |
| ... | ... |
(anything you find in the code coming from the settings dictionary)

### Expressions
| regex | message | embed_name
|--|--|--|
| [trigger expression in regex] | [resulting message, w\variable injections] | [embeds reference] |


### Embeds
| name | url |
|--|--|
| [embed name] | [embed url] |


### Commands
Messages starting with a specified escape ('.' by default) will attempt to trigger a command.

TODO: List of commands

### University system integration
We're working to integrate this bot with our amazing (maybe not that much) university system (CLIP).
In case you have no clue of what it is then any reference to it means "avoid at all costs".

There is a crawler to download every relevant public information out of CLIP in order to populate the bot database (and update it when needed).
It requires student credentials.
Even if you're a student and have the all mighty credentials, its probably not something you'll want to try.

If you're brave and want to try it, or want that information for something else (please don't be evil), it takes several hours to bootstrap the database, and it's a somewhat error-prone process, which for the bot was done step-by-step, one entity at a time.
Avoid doing so during times that might disturb other students access to CLIP. This thing does about 2-3 requests every second. 1AM-6AM(GMT) its probably your best shot.