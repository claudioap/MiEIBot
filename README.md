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
