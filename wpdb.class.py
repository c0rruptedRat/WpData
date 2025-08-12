import mysql.connector
import os
import json
from dotenv import load_dotenv

load_dotenv()

class WPDB():

    def __init__(self):
        self.connection = mysql.connector.connect(
            host=os.getenv("DB-HOST"),
            user=os.getenv("DB-USER"),
            password=os.getenv("DB-PW"),
            database=os.getenv("DB-NAME")
            )
        
        cursor = self.connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(tables)
        if tables.__len__() != 6:
            cursor.execute(""" 
                CREATE TABLE IF NOT EXISTS `Players` (
                `id` integer PRIMARY KEY,
                `latest_data` integer
                );

                CREATE TABLE IF NOT EXISTS `Guilds` (
                `id` integer PRIMARY KEY,
                `latest_data` integer
                );

                CREATE TABLE IF NOT EXISTS `PlayerData` (
                `id` integer PRIMARY KEY AUTO_INCREMENT,
                `day` date,
                `created_at` datetime,
                `uid` integer,
                `username` varchar(255),
                `wid` integer
                `gid` integer,
                `lv` integer,
                `cid` integer,
                `die` integer,
                `farmed` bigint,
                `max_power` integer,
                `camp_power` integer,
                `tech_power` integer,
                `unit_power` integer,
                `officer_power` integer,
                `army_air_power` integer,
                `army_navy_power` integer,
                `army_ground_power` integer,
                `tactic_card_power` integer,
                `mine_vehicle_power` integer,
                `super_computer_power` integer,
                `user_city_building_power` integer,
                `Kills1` integer,
                `Kills2` integer,
                `Kills3` integer,
                `Kills4` integer,
                `Kills5` integer,
                `Kills6` integer,
                `Kills7` integer,
                `Kills8` integer,
                `Kills9` integer,
                `Kills10` integer,
                `Kills11` integer,
                `Kills12` integer,
                `Kills13` integer,
                `Kills14` integer,
                `Kills15` integer,
                `TechContributions` integer,
                `Assists` integer
                );

                CREATE TABLE IF NOT EXISTS `GuildsData` (
                `id` integer PRIMARY KEY AUTO_INCREMENT,
                `gid` integer,
                `wid` integer,
                `sname` varchar(4),
                `fname` varchar(255),
                `owner` varchar(255),
                `day` date,
                `created_at` datetime
                );

                CREATE TABLE IF NOT EXISTS `Conquests` (
                `id` integer PRIMARY KEY AUTO_INCREMENT,
                `start_date` date,
                `end_date` date
                );

                CREATE TABLE IF NOT EXISTS `ConquestGuilds` (
                `id` integer PRIMARY KEY AUTO_INCREMENT,
                `cid` integer,
                `gid` integer
                );

                ALTER TABLE `Players` ADD FOREIGN KEY (`latest_data`) REFERENCES `PlayerData` (`id`);

                ALTER TABLE `Guilds` ADD FOREIGN KEY (`latest_data`) REFERENCES `GuildsData` (`id`);

                ALTER TABLE `PlayerData` ADD FOREIGN KEY (`uid`) REFERENCES `Players` (`id`);

                ALTER TABLE `PlayerData` ADD FOREIGN KEY (`gid`) REFERENCES `Guilds` (`id`);

                ALTER TABLE `GuildsData` ADD FOREIGN KEY (`gid`) REFERENCES `Guilds` (`id`);

                ALTER TABLE `ConquestGuilds` ADD FOREIGN KEY (`cid`) REFERENCES `Conquests` (`id`);

                ALTER TABLE `ConquestGuilds` ADD FOREIGN KEY (`gid`) REFERENCES `Guilds` (`id`);
            """)

    def add_player_data(self, playerData: list[dict]):
        if not playerData or not playerData["pid"]: return
        
        cursor = self.connection.cursor()
        fetch_player = "SELECT * FROM players WHERE id = %s"
        cursor.execute(fetch_player, (playerData["pid"], ))
        player = cursor.fetchone()
        print(player)
        if not player:
            create_new_player = "INSERT INTO players (id) VALUES (%s)"
            cursor.execute(create_new_player, (playerData["pid"], ))
            self.connection.commit()
        fetch_latest_data = "SELECT * FROM playerdata WHERE id = %s AND day = %s"
        cursor.execute(fetch_latest_data, (playerData["pid"], playerData["day"]))
        data = cursor.fetchone()
        if data:
            print("Data already in db:", data)
            return
        add_data = """INSERT INTO playerdata (
            day, 
            created_at, 
            uid, 
            username,
            wid,
            gid, 
            lv, 
            die, 
            farmed, 
            max_power,
            camp_power,
            tech_power,
            unit_power,
            officer_power,
            army_air_power,
            army_navy_power,
            army_ground_power,
            tactic_card_power,
            mine_vehicle_power,
            super_computer_power,
            user_city_building_power,
            Kills1,
            Kills2,
            Kills3,
            Kills4,
            Kills5,
            Kills6,
            Kills7,
            Kills8,
            Kills9,
            Kills10,
            Kills11,
            Kills12,
            Kills13,
            Kills14,
            Kills15,
            TechContributions,
            Assists) 
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        new_data=(playerData["day"],
                  playerData["created_at"],
                  playerData["pid"],
                  playerData["nick"],
                  playerData["wid"],
                  playerData["gid"],
                  playerData["lv"],
                  playerData["die"],
                  playerData["caiji"],
                  playerData["maxpower"],
                  playerData["powers"]["camp"],
                  playerData["powers"]["tech"],
                  playerData["powers"]["equip"],
                  playerData["powers"]["officer"],
                  playerData["powers"]["army_air"],
                  playerData["powers"]["army_navy"],
                  playerData["powers"]["army_ground"],
                  playerData["powers"]["tactic_card"],
                  playerData["powers"]["mine_vehicle"],
                  playerData["powers"]["super_computer"],
                  playerData["powers"]["user_city_building"],
                  playerData["kills"][0],
                  playerData["kills"][1],
                  playerData["kills"][2],
                  playerData["kills"][3],
                  playerData["kills"][4],
                  playerData["kills"][5],
                  playerData["kills"][6],
                  playerData["kills"][7],
                  playerData["kills"][8],
                  playerData["kills"][9],
                  playerData["kills"][10],
                  playerData["kills"][11],
                  playerData["kills"][12],
                  playerData["kills"][13],
                  playerData["kills"][14],
                  playerData["gx"],
                  playerData["bz"]
        )
        cursor.execute(add_data, new_data)
        self.connection.commit()

    def add_guild_data(self, guildData: list[dict]):
        if not guildData or guildData["gid"]: return 
        
        cursor = self.connection.cursor()
        find_guild = "SELECT * FROM guilds WHERE id = %s"
        cursor.execute(find_guild, (guildData["gid"],))
        guild = cursor.fetchone()
        print(guild)
        if not guild:
            create_new_player = "INSERT INTO guilds (id) VALUES (%s)"
            cursor.execute(create_new_player, (guildData["gid"], ))
            self.connection.commit()


        

    
DB = WPDB()

jsonstringplayer = '{"Code":0,"Message":"ok","Data":[{"id":200666099,"day":20250810,"pid":15981985,"wid":32,"cid":30020,"ccid":30020,"gid":1430842,"gnick":"AFK","lv":32,"nick":"c0rrupted","power":360417610,"maxpower":361686750,"sumkill":2555014,"score":1436745885,"die":953101,"caiji":19093877082,"gx":28235,"bz":18403,"c_power":28968,"c_die":0,"c_score":0,"c_sumkill":0,"c_caiji":35049033,"powers":{"camp":2223900,"tech":97210991,"equip":5327844,"total":360417610,"officer":14391300,"army_air":51621258,"army_navy":7430145,"army_ground":133859230,"tactic_card":805000,"mine_vehicle":16000,"super_computer":1692287,"user_city_building":45839655},"kills":[6094,6396,2357,5483,18519,55347,135086,211754,277656,295909,166464,180117,343383,314879,535570],"created_at":"2025-08-11 12:09:13"}]}'
jsonstringguild = '{"id":9946446,"day":20250810,"wid":32,"ccid":0,"gid":1430842,"power":960558834,"sname":"AFK","fname":"Aim.Fire.Kill.","owner":"MiniManny","kil":624333,"di":503,"c_power":226892,"c_kil":0,"created_at":"2025-08-11 12:16:17","c_di":0}'
#print(json.loads(jsonstring)["Data"][0])

DB.add_guild_data(json.loads(jsonstringguild))