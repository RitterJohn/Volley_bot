import sqlite3 as sl
import telebot
from threading import Thread
import schedule
import contextlib
import pandas as pd
from time import sleep
import prettytable as pt
import logging
from datetime import date

log_file = f'PATH_TO_LOG/log/{str(date.today())}.txt'
db = 'db_file.db'
TOKEN = 'TOKEN'

logging.basicConfig(level=logging.INFO, filename=log_file, format="%(asctime)s %(levelname)s %(message)s")

logging.info("START")

bot = telebot.TeleBot(TOKEN)

def send_message(mes, league):
    with contextlib.closing(sl.connect(db)) as connection:
        cursor = connection.cursor()
        ids = cursor.execute("SELECT distinct telegram_id FROM users JOIN subscriptions on user_id = user WHERE league = ?", (league, )).fetchall()

    errors = 0

    for id in ids:

        try:
            bot.send_message(id[0], mes, parse_mode='HTML')
        except:
            logging.error(f"send_message_error, {id}", exc_info=True)
            errors += 1
        sleep(0.4) # Чтобы обойти ограничение на отправку сообщений

    logging.info(f'send_message: {len(ids)} / {errors}')

def parse_games(url, gid):
    # Получение списка игр в определённой лиге
    parse_url = 'https://docs.google.com/spreadsheets/d/' + url + '/export?gid=' + gid + '&format=csv'
    df = pd.read_csv(parse_url)

    df = df.drop(columns=['ПРОТОКОЛ', 'ВРЕМЯ', 'СПОРТЗАЛ', 'СУДЬЯ', 'СУДЬЯ.1', 'КОММЕНТАРИИ'])
    df.dropna(subset=['СЧЁТ'], inplace=True)

    df['КОМАНДА ХОЗЯИН'] = df['КОМАНДА ХОЗЯИН'].apply(lambda x: x[1:] if x[0] == '⭐' else x)
    df['КОМАНДА ГОСТЬ'] = df['КОМАНДА ГОСТЬ'].apply(lambda x: x[1:] if x[0] == '⭐' else x)

    # Заменяем скобку (на всякий случай из-за ошибок в данных)
    df['КОМАНДА ХОЗЯИН'] = df['КОМАНДА ХОЗЯИН'].apply(lambda x: x.replace('{', '['))
    df['КОМАНДА ГОСТЬ'] = df['КОМАНДА ГОСТЬ'].apply(lambda x: x.replace('{', '['))

    # Потому что место в таблице не резиновое, сокращаем длину названия команды (в беседах таблица не всегда помещается)
    df['КОМАНДА ХОЗЯИН'] = df['КОМАНДА ХОЗЯИН'].apply(lambda x: x.replace('[F]', ''))
    df['КОМАНДА ГОСТЬ'] = df['КОМАНДА ГОСТЬ'].apply(lambda x: x.replace('[F]', ''))

    df['СЧЁТ'] = df['СЧЁТ'].apply(lambda x: round(x))
    df['Unnamed: 6'] = df['Unnamed: 6'].apply(lambda x: round(x))
    l = df.values.tolist()

    return l

def update():
    logging.info("UPDATE")
    with contextlib.closing(sl.connect(db)) as connection:
        cursor = connection.cursor()
        leagues = cursor.execute("SELECT * FROM leagues").fetchall()

    for league in leagues:

        flag = False

        with contextlib.closing(sl.connect(db)) as connection:
            cursor = connection.cursor()
            old_games = cursor.execute("SELECT date, team_name, (select team_name FROM teams WHERE team_id = team_2) as team_2, s1, s2 FROM games JOIN teams ON team_1 = team_id WHERE league = ?", (league[0], )).fetchall()
       
        games = parse_games(league[3], league[4])

        old_games = list(map(list, old_games))
        old_games_2 = [x[:3] for x in old_games]
        
        for game in games:

            if game not in old_games:
                
                flag = True

                with contextlib.closing(sl.connect(db)) as connection:
                    cursor = connection.cursor()

                    try:
                    
                        team_1 = cursor.execute("SELECT team_id FROM teams WHERE team_name = ? and league = ?", (game[1], league[0])).fetchall()[0][0]
                        team_2 = cursor.execute("SELECT team_id FROM teams WHERE team_name = ? and league = ?", (game[2], league[0])).fetchall()[0][0]

                    except:
                        logging.error(f"team_name_error, {team_1}, {team_2}", exc_info=True)
                        continue
                
                print(game)
                if game[:3] in old_games_2:
                    with contextlib.closing(sl.connect(db)) as connection:
                        cursor = connection.cursor()
                        cursor.execute("UPDATE games set s1 = ?, s2 = ? WHERE date = ? and team_1 = ? and team_2 = ?", (game[3], game[4], game[0], team_1, team_2))
                        connection.commit()
                    send_message(f'⚠ Изменения в игре ({game[0]}):\n{league[1]} ({league[2]})\n\n{game[1]} - {game[2]}\nСчёт {game[3]} : {game[4]}', league[0])              
                
                else:
                    with contextlib.closing(sl.connect(db)) as connection:
                        cursor = connection.cursor()

                        print(game[0], team_1, team_2, game[3], game[4])
                        cursor.execute("INSERT INTO games (date, team_1, team_2, s1, s2) VALUES (?, ?, ?, ?, ?)",  (game[0], team_1, team_2, game[3], game[4]))                
                        
                        connection.commit()
                    
                    send_message(f'✅ Новая игра ({game[0]}):\n{league[1]} ({league[2]})\n\n{game[1]} - {game[2]}\nСчёт {game[3]} : {game[4]}', league[0])
    
def rating_2(league):
    with contextlib.closing(sl.connect(db)) as connection:
        cursor = connection.cursor()
        games = cursor.execute("SELECT team_name, (select team_name FROM teams WHERE team_id = team_2) as team_2, s1, s2 FROM games JOIN teams ON team_1 = team_id WHERE league = ?", (league, )).fetchall()

    points = {}
    
    for game in games:

        if game[2] > game[3]:
            winner, looser = game[0], game[1]
        else:
            winner, looser = game[1], game[0]

        if abs(game[3] - game[2]) == 1:
            winner_p, looser_p = 2, 1
        else:
            winner_p, looser_p = 3, 0

        if winner in points.keys():
            points[winner] = [points[winner][0] + 1, points[winner][1] + winner_p, points[winner][2] + 1]
        else:
            points[winner] = [1, winner_p, 1]

        if looser in points.keys():
            points[looser] = [points[looser][0], points[looser][1] + looser_p, points[looser][2] + 1]
        else:
            points[looser] = [0, looser_p, 1]

    first_rating = list(points.items())

    rating = [[x[0], x[1][0], x[1][1], x[1][2]] for x in first_rating]

    rating.sort(key = lambda row: (row[1], row[2]), reverse=True)

    table = pt.PrettyTable(['№', 'Команда', 'Поб.', 'Очки', 'Игры'])

    table.align['№'] = 'r'
    table.align['Команда'] = 'l'
    table.align['Очки'] = 'r'
    table.align['Игры'] = 'r'

    place = 0

    for team, win, points, games in rating:
        place += 1
        table.add_row([place, team[:15], win, points, games])

    mess = f'<pre>{table}</pre>'
    
    return mess

def preview(url, gid):
    parse_url = 'https://docs.google.com/spreadsheets/d/' + url + '/export?gid=' + gid + '&format=csv'
    df = pd.read_csv(parse_url)

    df = df.drop(columns=['ПРОТОКОЛ', 'СПОРТЗАЛ', 'СУДЬЯ', 'СУДЬЯ.1', 'КОММЕНТАРИИ'])
    df.dropna(subset=['КОМАНДА ХОЗЯИН'], inplace=True)
    
    df['КОМАНДА ХОЗЯИН'] = df['КОМАНДА ХОЗЯИН'].apply(lambda x: x[1:] if x[0] == '⭐' else x)
    df['КОМАНДА ГОСТЬ'] = df['КОМАНДА ГОСТЬ'].apply(lambda x: x[1:] if x[0] == '⭐' else x)

    # Заменяем скобку (на всякий случай из-за ошибок в данных)
    df['КОМАНДА ХОЗЯИН'] = df['КОМАНДА ХОЗЯИН'].apply(lambda x: x.replace('{', '['))
    df['КОМАНДА ГОСТЬ'] = df['КОМАНДА ГОСТЬ'].apply(lambda x: x.replace('{', '['))

    # Потому что место в таблице не резиновое, сокращаем длину названия команды (в беседах таблица не всегда помещается)
    df['КОМАНДА ХОЗЯИН'] = df['КОМАНДА ХОЗЯИН'].apply(lambda x: x.replace('[F]', ''))
    df['КОМАНДА ГОСТЬ'] = df['КОМАНДА ГОСТЬ'].apply(lambda x: x.replace('[F]', ''))

    null_data = df[df.isnull().any(axis=1)]
    null_data = null_data.drop(columns=['СЧЁТ', 'Unnamed: 6'])

    l = null_data.values.tolist()

    return l


def schedule_checker():
    while True:
        schedule.run_pending()

@bot.message_handler(commands=['start'])
def add_user(message):

    with contextlib.closing(sl.connect(db)) as connection:
      cursor = connection.cursor()
      ids = cursor.execute("SELECT user_id FROM users WHERE telegram_id = ?", (message.chat.id, )).fetchall()
      leagues = cursor.execute("SELECT * FROM leagues").fetchall()

    if len(ids) == 0:
        with contextlib.closing(sl.connect(db)) as connection:
            cursor = connection.cursor()
            cursor.execute("INSERT INTO users (telegram_id) VALUES (?)",  (message.chat.id, )).fetchall()
            connection.commit()
   
    logging.info("new user")
    
    leagues_list = ""
    leagues_list = leagues_list.join([f'{i+1}) {leagues[i][1]} ({leagues[i][2]})\n' for i in range(len(leagues))])
    
    bot.send_message(message.chat.id, 'Этот бот отправляет уведомления о прошедших матчах чемпионата Екатеринбурга по волейболу. Команда /help поможет разобраться.')
    bot.send_message(message.chat.id, f'Выберите интересующие Вас лиги с помощью команды /add:\n{leagues_list}\nНапример, "/add 2" или "/add 4 6 2"')

@bot.message_handler(commands=['add'])
def choose_leagues(message):
    with contextlib.closing(sl.connect(db)) as connection:
      cursor = connection.cursor()
      leagues = cursor.execute("SELECT * FROM leagues").fetchall()
      user = cursor.execute("SELECT user_id FROM users WHERE telegram_id = ?", (message.chat.id, )).fetchall()[0][0]
    
    try:
        text = message.text.split()[1:]
        new_list = []
        for number in text:
            number = int(number)

            if number > 0 and number < len(leagues) + 1:
                new_list.append(number)

        isValid = True
        
    except:
        bot.send_message(message.chat.id, 'Неправильный формат, введите номера лиг после команды. Например, "/add 1 5 8"')
        isValid = False

    if isValid:
        new_list.sort()
        new_list = list(set(new_list))

        values = [(user, league) for league in new_list]
        # print(values)
        
        with contextlib.closing(sl.connect(db)) as connection:
            cursor = connection.cursor()
            cursor.execute("DELETE FROM subscriptions WHERE user = ?", (user, ))
            cursor.executemany("INSERT INTO subscriptions (user, league) VALUES (?, ?)", values)
            connection.commit()

        message_text = f'{leagues[new_list[0] - 1][1]} ({leagues[new_list[0] - 1][2]})'

        for i in range(1, len(new_list)):
            message_text = message_text + f',\n{leagues[new_list[i] - 1][1]}, ({leagues[new_list[i] - 1][2]})'

        bot.send_message(message.chat.id, f'Вы подписались на следующие лиги: {message_text}')

@bot.message_handler(commands=['remove'])
def remove_leagues(message):
    with contextlib.closing(sl.connect(db)) as connection:
        cursor = connection.cursor()
        user = cursor.execute("SELECT user_id FROM users WHERE telegram_id = ?", (message.chat.id, )).fetchall()[0][0]
        cursor.execute("DELETE FROM subscriptions WHERE user = ?", (user, ))
        connection.commit()

    bot.send_message(message.chat.id, f'Вы отказались от рассылки. Если хотите вновь получать уведомления, добавьте лиги с помощью команды /add')
    logging.info("departed user")

@bot.message_handler(commands=['rating'])
def get_rating(message):
    with contextlib.closing(sl.connect(db)) as connection:
        cursor = connection.cursor()
        user = cursor.execute("SELECT user_id FROM users WHERE telegram_id = ?", (message.chat.id, )).fetchall()[0][0]
        leagues = cursor.execute("SELECT league, league_name, type FROM subscriptions JOIN leagues ON league = league_id WHERE user = ?", (user, )).fetchall()

    for league in leagues:

        mess = f'{league[1]} ({league[2]})\n{rating_2(league[0])}'

        bot.send_message(message.chat.id, mess, parse_mode='HTML')  

@bot.message_handler(commands=['preview'])
def get_preview(message):
    with contextlib.closing(sl.connect(db)) as connection:
        cursor = connection.cursor()
        leagues = cursor.execute("SELECT league, league_name, type, url, gid FROM subscriptions JOIN leagues ON league = league_id JOIN users on user = user_id WHERE telegram_id = ?", (message.chat.id, )).fetchall()

    for league in leagues:
        games = preview(league[3], league[4])

        if len(games) > 0:
        
            message_text = f'{league[1]} ({league[2]}):\n'
            
            for game in games:
                message_text += f'{game[0]} ({game[1]}): {game[2]} - {game[3]}\n'

            bot.send_message(message.chat.id, message_text[:-1])

@bot.message_handler(commands=['list'])
def get_list(message):
    
    with contextlib.closing(sl.connect(db)) as connection:
      cursor = connection.cursor()
      leagues = cursor.execute("SELECT * FROM leagues").fetchall()

    leagues_list = ""
    leagues_list = leagues_list.join([f'{i+1}) {leagues[i][1]} ({leagues[i][2]})\n' for i in range(len(leagues))])

    bot.send_message(message.chat.id, leagues_list)

@bot.message_handler(commands=['my_list'])
def get_my_list(message):
    with contextlib.closing(sl.connect(db)) as connection:
        cursor = connection.cursor()
        leagues = cursor.execute("SELECT league_id, league_name, type FROM subscriptions JOIN leagues ON league = league_id JOIN users on user = user_id WHERE telegram_id = ?", (message.chat.id, )).fetchall()

    leagues_list = ""
    leagues_list = leagues_list.join([f'{i+1}) {leagues[i][1]} ({leagues[i][2]})\n' for i in range(len(leagues))])

    bot.send_message(message.chat.id, leagues_list)

@bot.message_handler(commands=['help'])
def help(message):
    message_text = '/add - подписаться на лиги (/add 1 5 9)\n/rating - турнирная таблица\n/preview - анонсы\n/remove - отписаться\n/list - все лиги\n/my_list - Ваши лиги'

    bot.send_message(message.chat.id, message_text)        

if __name__ == "__main__":

    schedule.every().day.at("08:00").do(update,)
    Thread(target=schedule_checker).start() 

bot.polling()