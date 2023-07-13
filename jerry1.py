import asyncio
import json
import os
import os.path
import datetime
from math import ceil
import pandas as pd
from sqlalchemy import create_engine

from tap_titans.providers.providers import *


with open("raid_config.json", mode="rt") as f:
    config = json.load(f)
AUTH_TOKEN = config["auth_token"]
PLAYER_TOKENS = config["player_tokens"]
authorized_clan = config["authorized_clan"]
my_conn = create_engine(f"mysql+mysqldb://{config['db_username']}:{config['db_password']}@{config['db_host']}/{config['db_name']}")


raid_start_time_dict = dict()

def cycle_calc(raid_start_time_string:str, attack_datetime_string:str) -> int:
    attack_datetime = datetime.datetime(int(attack_datetime_string[0:4]), int(attack_datetime_string[5:7]), int(
        attack_datetime_string[8:10]), int(attack_datetime_string[11:13]), int(attack_datetime_string[14:16]), int(attack_datetime_string[17:19]))
    raid_start_time = datetime.datetime(int(raid_start_time_string[0:4]), int(raid_start_time_string[5:7]), int(
        raid_start_time_string[8:10]), int(raid_start_time_string[11:13]), int(raid_start_time_string[14:16]), int(raid_start_time_string[17:19]))
    dt = attack_datetime - raid_start_time
    cycle = max(dt.days * 2 + ceil(dt.seconds/(86400/2)), 1)
    return cycle

def return_new_attack_dict(input_dict:dict, raid_start_time_string:str=None)->dict:
    '''raid start time is used to calc the cycle number, as it's not present on the input_dict'''
    keys = ["raid_id", "time", "clan_code", "cycle_number", "total_damage", "player_name", "player_code", "titan1_index", "titan1_BodyHead", "titan1_ArmorHead", "titan1_SkeletonHead", "titan1_BodyChestUpper", "titan1_ArmorChestUpper", "titan1_SkeletonChestUpper", "titan1_BodyArmUpperRight", "titan1_ArmorArmUpperRight", "titan1_SkeletonArmUpperRight", "titan1_BodyArmUpperLeft", "titan1_ArmorArmUpperLeft", "titan1_SkeletonArmUpperLeft", "titan1_BodyLegUpperRight", "titan1_ArmorLegUpperRight", "titan1_SkeletonLegUpperRight", "titan1_BodyLegUpperLeft", "titan1_ArmorLegUpperLeft", "titan1_SkeletonLegUpperLeft", "titan1_BodyHandRight", "titan1_ArmorHandRight", "titan1_SkeletonHandRight", "titan1_BodyHandLeft", "titan1_ArmorHandLeft", "titan1_SkeletonHandLeft", "titan1_total", "titan2_index", "titan2_BodyHead", "titan2_ArmorHead", "titan2_SkeletonHead", "titan2_BodyChestUpper", "titan2_ArmorChestUpper", "titan2_SkeletonChestUpper", "titan2_BodyArmUpperRight", "titan2_ArmorArmUpperRight", "titan2_SkeletonArmUpperRight", "titan2_BodyArmUpperLeft",
            "titan2_ArmorArmUpperLeft", "titan2_SkeletonArmUpperLeft", "titan2_BodyLegUpperRight", "titan2_ArmorLegUpperRight", "titan2_SkeletonLegUpperRight", "titan2_BodyLegUpperLeft", "titan2_ArmorLegUpperLeft", "titan2_SkeletonLegUpperLeft", "titan2_BodyHandRight", "titan2_ArmorHandRight", "titan2_SkeletonHandRight", "titan2_BodyHandLeft", "titan2_ArmorHandLeft", "titan2_SkeletonHandLeft", "titan2_total", "player_raid_level", "tap_damage", "card1_id", "card1_damage", "card1_level", "card2_id", "card2_damage", "card2_level", "card3_id", "card3_damage", "card3_level", "attacks_remaining", "current_titan_index", "current_enemy_id", "current_BodyHead", "current_ArmorHead", "current_BodyChestUpper", "current_ArmorChestUpper", "current_BodyArmUpperRight", "current_ArmorArmUpperRight", "current_BodyArmUpperLeft", "current_ArmorArmUpperLeft", "current_BodyLegUpperRight", "current_ArmorLegUpperRight", "current_BodyLegUpperLeft", "current_ArmorLegUpperLeft", "current_BodyHandRight", "current_ArmorHandRight", "current_BodyHandLeft", "current_ArmorHandLeft", "current_hp"]
    ret_dict = {key: 0 for key in keys}

    ret_dict["raid_id"] = input_dict["raid_id"]
    ret_dict["time"] = input_dict["attack_log"]["attack_datetime"]
    ret_dict["clan_code"] = input_dict["clan_code"]
    ret_dict["player_name"] = input_dict["player"]["name"]
    ret_dict["player_code"] = input_dict["player"]["player_code"]

    # get the card id and card level of the card used
    cards = input_dict["attack_log"]["cards_level"]
    for i, card in enumerate(cards):
        ret_dict[f"card{i+1}_id"] = card["id"]
        ret_dict[f"card{i+1}_level"] = card["value"]

    # edge case: titan kill
    damage_logs = input_dict["attack_log"]["cards_damage"]
    ret_dict["titan1_index"] = damage_logs[0]["titan_index"]
    ret_dict["titan2_index"] = -1

    for i in damage_logs:
        if i["titan_index"] != ret_dict["titan1_index"]:
            ret_dict["titan2_index"] = i["titan_index"]
            break
    ret_dict["titan1_index"] = ret_dict["titan1_index"]

    # part damage for each part, card damage for each card
    for damage in damage_logs:
        for part_damage in damage["damage_log"]:
            ret_dict["total_damage"] += part_damage["value"]
            part_id = part_damage["id"]
            if damage["titan_index"] == ret_dict["titan1_index"]:
                ret_dict[f"titan1_{part_id}"] += part_damage["value"]
                ret_dict["titan1_total"] += part_damage["value"]
            else:
                ret_dict[f"titan2_{part_id}"] += part_damage["value"]
                ret_dict["titan2_total"] += part_damage["value"]

        if damage["id"] == None:
            for dmg in damage["damage_log"]:
                ret_dict["tap_damage"] += dmg["value"]
        else:
            for i in range(3):
                if damage["id"] == ret_dict[f"card{i+1}_id"]:
                    for dmg in damage["damage_log"]:
                        ret_dict[f"card{i+1}_damage"] += dmg["value"]
                    break

    # string; like "2023-06-19T15:29:44Z"
    # calc attack cycle
    if raid_start_time_string != None:
        attack_datetime_string = input_dict["attack_log"]["attack_datetime"]
        cycle = cycle_calc(raid_start_time_string, attack_datetime_string)
        ret_dict["cycle_number"] = cycle

    ret_dict["attacks_remaining"] = input_dict["player"]["attacks_remaining"]
    ret_dict["player_raid_level"] = input_dict["player"]["raid_level"]

    ret_dict["current_titan_index"] = input_dict["raid_state"]["titan_index"]
    ret_dict["current_enemy_id"] = input_dict["raid_state"]["current"]["enemy_id"]
    ret_dict["current_hp"] = input_dict["raid_state"]["current"]["current_hp"]

    current_parts = input_dict["raid_state"]["current"]["parts"]
    for i in current_parts:
        ret_dict[f"current_{i['part_id']}"] = i["current_hp"]

    return ret_dict


# This is just an example function that takes anything or nothing.
# Annotations provide what should be accepted for each event
async def generic_log_storage(anything=None)->None:
    if anything != None:
        if anything["clan_code"] not in authorized_clan:
            return
        dir_path = f"./r{anything['raid_id']}logs"
        if not os.path.isdir(dir_path):
            os.mkdir(dir_path)
        counter = len([entry for entry in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, entry))])
        out_file = open(f"{dir_path}/{(counter+1):04}_{anything['clan_code']}.json", "w")
        json.dump(anything, out_file, indent=2)
        out_file.close()

def add_to_raid_start_dict(raid_id:int, timestamp:str)->None:
    global raid_start_time_dict
    raid_start_time_dict[raid_id] = timestamp
    print(raid_start_time_dict)


async def pre_raid_start_log(raid_start_log:dict)->None:
    if raid_start_log["clan_code"] not in authorized_clan:
        print("no")
        return
    add_to_raid_start_dict(raid_id=raid_start_log["raid_id"], timestamp=raid_start_log["start_at"])
    await generic_log_storage(raid_start_log)

async def mid_raid_start_log(raid_start_log:dict)->None:
    if raid_start_log["clan_code"] not in authorized_clan:
        print("no")
        return
    add_to_raid_start_dict(raid_id=raid_start_log["raid_id"], timestamp=raid_start_log["raid_started_at"])
    await generic_log_storage(raid_start_log)


# toss a bunch of files into the r{raid_id} folder
def store_data(json_data:dict)->None:
    dir_path = f"./r{json_data['raid_id']}"
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
    counter = len([entry for entry in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, entry))])
    out_file = open(f"{dir_path}/{(counter+1):04}_{json_data['player']['player_code']}.json", "w")
    json.dump(json_data, out_file, indent=2)
    out_file.close()


def db_stuff(json_data:dict)->None:
    global raid_start_time_dict
    new_attack_dict = return_new_attack_dict(json_data,raid_start_time_dict[json_data["raid_id"]])
    df = pd.DataFrame([new_attack_dict])
    df.to_sql(con=my_conn, name=f"r{json_data['raid_id']}", if_exists="append")

# Define the event handler function to process received data

async def handle_attack_data(data:dict)->None:
    # print(data)  # Optional: Print the received data for debugging
    # Call the store_data function to store the received data
    store_data(data)
    # all the db operations happens here
    db_stuff(data)


# Modify the existing test function to use the new event handler
async def attack_log(att_dict:dict=None)->None:
    await handle_attack_data(att_dict)

# We have to subscribe after we connect
async def connected(anything=None)->None:
    print("Connected")

    r = RaidRestAPI(AUTH_TOKEN)

    resp = await r.subscribe(PLAYER_TOKENS)
    if len(resp.refused) > 0:
        print("Failed to subscribe to clan with reason:",
              resp.refused[0].reason)
    else:
        print("Subscribed to clan:", resp.ok[0].clan_code)

async def err(anything:dict=None)->None:
    if anything != None:
        dir_path = "./error_log"
        if not os.path.isdir(dir_path):
            os.mkdir(dir_path)
        counter = len([entry for entry in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, entry))])
        out_file = open(f"{dir_path}/{(counter+1):04}.json", "w")
        json.dump(anything, out_file, indent=2)
        out_file.close()

wsc = WebsocketClient(
    connected=connected,
    disconnected=err,
    error=err,
    connection_error=err,
    clan_removed=err,
    raid_attack=attack_log,
    raid_start=generic_log_storage,
    clan_added_raid_start=pre_raid_start_log,
    raid_end=generic_log_storage,
    raid_retire=generic_log_storage,
    raid_cycle_reset=generic_log_storage,
    clan_added_cycle=mid_raid_start_log,
    raid_target_changed=generic_log_storage,
    setting_validate_arguments=False,
)

asyncio.run(wsc.connect(AUTH_TOKEN))
