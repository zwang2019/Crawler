# Web Spider for AFL Data
# MIT LICENSE
# Author: Z.WANG
# Date: 07/Apr/2022

# import libraries
import pandas as pd
import requests
from fake_useragent import UserAgent
from time import sleep
import random

from bs4 import BeautifulSoup
import re

import warnings
from tqdm import tqdm

# Supresses scientific notation
pd.set_option('display.float_format', lambda x: '%.2f' % x)
# display the full tables
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

warnings.filterwarnings('ignore')

debug = False
random.seed(666)

# Query and Insert the old database !
games = pd.read_csv(r'./data/games.csv')
players = pd.read_csv(r'./data/players.csv')
stats = pd.read_csv(r'./data/stats.csv')

if debug is True:
    games.head()

if debug is True:
    players.head()

if debug is True:
    stats.head()


# Query Functions

def query_player_info(url):
    # Crawling Website
    url_player = url
    # Fake a user header
    headers = {'User-Agent': UserAgent().random}

    player_datas = requests.get(url=url_player, headers=headers)
    html_players = player_datas.text
    # For Loop crawling, time interval is demanded.
    sleep(0.25)
    soup_players = BeautifulSoup(html_players, 'lxml')
    c_play_infos = soup_players.find_all('center')

    temp_player_info = re.match(r'.*?Born:(\d*-\w*-\d*) \(Debut.*?Height:(\d*).*?Weight:(\d*) kg', c_play_infos[0].text,
                                flags=re.S)

    player_dob = temp_player_info.group(1)
    player_height = temp_player_info.group(2)
    player_weight = temp_player_info.group(3)

    return [player_dob, int(player_height), int(player_weight)]


def query_stats_info(new_stats_data, new_players_data, url, gameid, round, home_team, away_team):
    p_data = new_players_data
    s_data = new_stats_data

    # Crawling Website
    url = url
    gameid = gameid
    round = round

    # Fake a user header
    headers = {'User-Agent': UserAgent().random}

    # query stats data
    stats_datas = requests.get(url=url, headers=headers)
    html_stats = stats_datas.text

    # For Loop crawling, time interval is demanded.
    sleep(0.25)

    soup_stats = BeautifulSoup(html_stats, 'lxml')
    c_stats_tables = soup_stats.find_all('table', attrs={'class': "sortable"})

    for i_table in range(0, 2):
        # each table start here
        if i_table == 0:
            player_team = home_team
        if i_table == 1:
            player_team = away_team

        table_body = c_stats_tables[i_table].find_all('tbody')
        table_rows = table_body[0].find_all('tr')

        for table_row in table_rows:
            # each row start here

            table_columns = table_row.find_all('td')
            for p in range(len(table_columns)):
                # each column start here
                infos = table_columns[p].text
                if p != 0 and p != 1:
                    if infos == '\xa0':
                        infos = 0

                    infos = int(infos)

                if p == 0:
                    infos = re.sub(r'\D', '', infos)
                    player_number = int(infos)
                    subs = '-'

                    table_players_rows = c_stats_tables[i_table + 2].find_all('tbody')[0].find_all('tr')
                    for table_players_row in table_players_rows:
                        table_players_columns = table_players_row.find_all('td')
                        if int(table_players_columns[p].text) == player_number:
                            gameNumber = re.match(r'(\d*) \(', table_players_columns[3].text)[1]
                            gameNumber = int(gameNumber)

                if p == 1:
                    displayName = infos

                    name_url = 'https://afltables.com/afl/stats' + table_columns[p].find_all('a', href=True)[0]['href'][
                                                                   5:]
                    team_player_info = query_player_info(name_url)
                    player_dob = team_player_info[0]
                    player_height = team_player_info[1]
                    player_weight = team_player_info[2]

                    selected_p = p_data[(p_data['displayName'] == displayName) & (pd.to_datetime(p_data['dob']) == pd.to_datetime(player_dob))]
                    # problem is dob

                    if selected_p.shape[0] == 0:
                        selected_p.at[0, 'playerId'] = int('2022' + str(random.randint(0, 999999)).zfill(6))
                        selected_p.at[0, 'displayName'] = displayName
                        selected_p.at[0, 'height'] = player_height
                        selected_p.at[0, 'weight'] = player_weight
                        selected_p.at[0, 'dob'] = player_dob
                        selected_p.at[0, 'position'] = 'To_be_insert_manually'
                        selected_p.at[0, 'origin'] = 'To_be_insert_manually'
                        p_data = pd.concat([selected_p, p_data])

                    playerId = selected_p['playerId'].iloc[0]

                if p == 2:
                    Kicks = infos
                if p == 3:
                    Marks = infos
                if p == 4:
                    Handballs = infos
                if p == 5:
                    Disposals = infos
                if p == 6:
                    Goals = infos
                if p == 7:
                    Behinds = infos
                if p == 8:
                    Hit_Outs = infos
                if p == 9:
                    Tackles = infos
                if p == 10:
                    Rebounds = infos
                if p == 11:
                    Inside_50s = infos
                if p == 12:
                    Clearances = infos
                if p == 13:
                    Clangers = infos
                if p == 14:
                    Frees = infos
                if p == 15:
                    Frees_Against = infos
                if p == 16:
                    Brownlow_Votes = infos
                if p == 17:
                    Contested_Possessions = infos
                if p == 18:
                    Uncontested_Possessions = infos
                if p == 19:
                    Contested_Marks = infos
                if p == 20:
                    Marks_Inside_50 = infos
                if p == 21:
                    One_Percenters = infos
                if p == 22:
                    Bounces = infos
                if p == 23:
                    Goal_Assists = infos
                if p == 24:
                    p_Played = infos

            temp_stats_row = pd.DataFrame()
            temp_stats_row.at[0, 'gameId'] = gameid
            temp_stats_row.at[0, 'team'] = player_team
            temp_stats_row.at[0, 'year'] = 2022
            temp_stats_row.at[0, 'round'] = round
            temp_stats_row.at[0, 'playerId'] = playerId
            temp_stats_row.at[0, 'displayName'] = displayName
            temp_stats_row.at[0, 'gameNumber'] = gameNumber
            temp_stats_row.at[0, 'Disposals'] = Disposals
            temp_stats_row.at[0, 'Kicks'] = Kicks
            temp_stats_row.at[0, 'Marks'] = Marks
            temp_stats_row.at[0, 'Handballs'] = Handballs
            temp_stats_row.at[0, 'Goals'] = Goals
            temp_stats_row.at[0, 'Behinds'] = Behinds
            temp_stats_row.at[0, 'Hit Outs'] = Hit_Outs
            temp_stats_row.at[0, 'Tackles'] = Tackles
            temp_stats_row.at[0, 'Rebounds'] = Rebounds
            temp_stats_row.at[0, 'Inside 50s'] = Inside_50s
            temp_stats_row.at[0, 'Clearances'] = Clearances
            temp_stats_row.at[0, 'Clangers'] = Clangers
            temp_stats_row.at[0, 'Frees'] = Frees
            temp_stats_row.at[0, 'Frees Against'] = Frees_Against
            temp_stats_row.at[0, 'Brownlow Votes'] = Brownlow_Votes
            temp_stats_row.at[0, 'Contested Possessions'] = Contested_Possessions
            temp_stats_row.at[0, 'Uncontested Possessions'] = Uncontested_Possessions
            temp_stats_row.at[0, 'Contested Marks'] = Contested_Marks
            temp_stats_row.at[0, 'Marks Inside 50'] = Marks_Inside_50
            temp_stats_row.at[0, 'One Percenters'] = One_Percenters
            temp_stats_row.at[0, 'Bounces'] = Bounces
            temp_stats_row.at[0, 'Goal Assists'] = Goal_Assists
            temp_stats_row.at[0, '% Played'] = p_Played
            temp_stats_row.at[0, 'Subs'] = '-'
            s_data = pd.concat([temp_stats_row, s_data])

    return s_data, p_data


def query_games_info(new_stats_data, new_players_data, new_games_data, crawl_range_start_round, crawl_range_end_round):
    # compile Re
    re_date_time_venue = re.compile(r'\w* (\d*-\w*-\d*) (\d*:\d*\s\w*) .*?Venue: (.*)')
    re_att = re.compile(r'Att: \d*,*\d*')
    re_find_player_info = re.compile(r'.*?Born:(\d*-\w*-\d*) \(Debut.*?Height:(\d*).*?Weight:(\d*) kg')

    # Crawling Website
    url = 'https://afltables.com/afl/seas/2022.html'
    # Fake a user header
    headers = {'User-Agent': UserAgent().random}

    # Request the data
    datas = requests.get(url=url, headers=headers)
    html = datas.text

    # For Loop crawling, time interval is demanded.
    sleep(1)

    soup = BeautifulSoup(html, 'lxml')
    c_games = soup.find_all('table', attrs={'style': 'font: 12px Verdana;', 'width': "100%", 'border': "1"})

    # each i is a table
    for i in tqdm(range(crawl_range_start_round * 10, crawl_range_end_round * 10)):

        if i % 10 != 9:

            tables = c_games[i].find_all('td')

            r = i // 10 + 1
            id = i % 10 + 1

            gameid = '2022R' + str(r) + '0' + str(id)
            year = 2022
            round = 'R' + str(r)

            for j in range(len(tables)):

                if j == 0:
                    home_team = tables[j].text
                    home_team_index = 'https://afltables.com/afl' + tables[j].find_all('a', href=True)[0]['href'][2:]

                if j == 2:
                    home_team_score = int(tables[j].text)

                if j == 3:
                    temp = re_date_time_venue.match(tables[j].text)
                    date = temp.group(1)
                    venue = temp.group(3)
                    start_time = temp.group(2)

                    attendance = re_att.findall(tables[j].text)
                    if not attendance:
                        attendance = ['0']
                    attendance = re.sub(r'\D', '', attendance[0])
                    attendance = int(attendance)

                if j == 4:
                    away_team = tables[j].text
                    away_team_index = 'https://afltables.com/afl' + tables[j].find_all('a', href=True)[0]['href'][2:]

                if j == 6:
                    away_team_score = int(tables[j].text)

                if j == 7:
                    stats_link = 'https://afltables.com/afl' + tables[j].find_all('a', href=True)[0]['href'][2:]
                    new_stats_data, new_players_data = query_stats_info(new_stats_data, new_players_data, stats_link,
                                                                        gameid, round, home_team, away_team)

            rainfall = 0

            temp_games_row = pd.DataFrame()
            temp_games_row.at[0, 'gameId'] = gameid
            temp_games_row.at[0, 'year'] = year
            temp_games_row.at[0, 'round'] = round
            temp_games_row.at[0, 'date'] = date
            temp_games_row.at[0, 'venue'] = venue
            temp_games_row.at[0, 'startTime'] = start_time
            temp_games_row.at[0, 'attendance'] = attendance
            temp_games_row.at[0, 'homeTeam'] = home_team
            temp_games_row.at[0, 'homeTeamScore'] = home_team_score
            temp_games_row.at[0, 'awayTeam'] = away_team
            temp_games_row.at[0, 'awayTeamScore'] = away_team_score
            temp_games_row.at[0, 'rainfall'] = rainfall
            new_games_data = pd.concat([temp_games_row, new_games_data])

    return new_stats_data, new_players_data, new_games_data


def query_prediction_format(query_round):
    submission = pd.DataFrame()
    # Crawling Website
    url = 'https://afltables.com/afl/seas/2022.html'
    # Fake a user header
    headers = {'User-Agent': UserAgent().random}

    # Request the data
    datas = requests.get(url=url, headers=headers)
    html = datas.text

    # For Loop crawling, time interval is demanded.
    sleep(1)

    soup = BeautifulSoup(html, 'lxml')
    c_games = soup.find_all('table', attrs={'style': 'font: 12px Verdana;', 'width': "100%", 'border': "1"})

    # each i is a table
    for i in tqdm(range((query_round - 1) * 10, query_round * 10)):

        if i % 10 != 9:

            tables = c_games[i].find_all('td')

            r = i // 10 + 1
            id = i % 10 + 1

            for j in range(len(tables)):

                if j == 0:
                    home_team = tables[j].text
                    home_team_index = 'https://afltables.com/afl' + tables[j].find_all('a', href=True)[0]['href'][2:]

                if j == 4:
                    away_team = tables[j].text
                    away_team_index = 'https://afltables.com/afl' + tables[j].find_all('a', href=True)[0]['href'][2:]

            temp_query_prediction_row = pd.DataFrame()
            temp_query_prediction_row.at[0, 'Host'] = home_team
            temp_query_prediction_row.at[0, 'Away'] = away_team

            submission = pd.concat([submission, temp_query_prediction_row])

    submission.to_csv('./output/r' + str(query_round) + '_submission.csv', index=False)
    return


new_players_data = players.copy()
new_stats_data = stats.copy()
new_games_data = games.copy()

# Change the query round:
new_stats_data, new_players_data, new_games_data = query_games_info(new_stats_data, new_players_data, new_games_data, 8,
                                                                    9)

new_stats_data.to_csv('./output/stats.csv', index=False)
new_players_data.to_csv('./output/players.csv', index=False)
new_games_data.to_csv('./output/games.csv', index=False)

# note this is the next round
query_round = 10
query_prediction_format(query_round)
