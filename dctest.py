import discord
import os
import json

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
with open("config.json") as f:
    stuff = json.load(f)
    token = stuff["discord-token"]

client.run(token)