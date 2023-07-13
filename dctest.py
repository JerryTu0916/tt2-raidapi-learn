import discord
import os

intent = discord.Intents.all()
client = discord.Client(intents=intent)

@client.event
async def on_ready():
    print(f"We are logged in as {client.user}")

@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        return

    if message.content.startswith('$hello'):
        await message.channel.send('Hello!')

# client.run(os.getenv('TOKEN'))
client.run("MTAxODg1MTUyMjgwMjYyNjYwMQ.GIuyEu.F9ecsAzt_6Ntcrk4KB_0YWkP1ORSEZK0d96ugg")