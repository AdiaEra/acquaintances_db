from pprint import pprint

import psycopg2.extras
from psycopg2.errors import UniqueViolation
import pandas as pd

from db import conn


def add_user(user_name: str, chat_id: int, nick_name, age: int, gender: str, photo, about_me: str, preferences: str,
             city: str, user_index: int):
    """
    Функция, позволяющая добавить нового пользователя в таблицу users
    :param user_name: имя пользователя
    :param chat_id: id чата пользователя
    :param nick_name: ник пользователя
    :param age: возраст пользователя
    :param gender: пол пользователя (мужчина, девушка, пара)
    :param photo: фото пользователя
    :param about_me: информатия пользователя о себе
    :param preferences: предпочтения пользователя (мужчина, девушка, пара)
    :param city: город, в котором проживает пользователь
    :param user_index: индекс пользователя
    :return: Новый пользователь добавлен (Пользователь с таким user_name уже есть в базе)
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT user_name FROM users Where user_name = %s""", (user_name,))
        cur.fetchone()
        if cur.fetchone() is None:
            try:
                cur.execute("""
                            INSERT INTO users (user_name, chat_id, nick_name, age, gender, photo, about_me, preferences, city, user_index)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                            (
                                user_name, chat_id, nick_name, age, gender, photo, about_me, preferences, city,
                                user_index))
            except UniqueViolation:
                return 'Пользователь с таким user_name уже есть в базе'
        return 'Новый пользователь добавлен'


us_name = 'Марина'
user_chat_id = 7
user_nick = 'mary'
user_age = 37
user_gender = 'девушка'
user_photo = ''
user_about_me = 'нравится рисовать'
user_preferences = 'мужчина'
user_city = 'Кузнецк'
us_index = 0
# print(add_user(us_name, user_chat_id, user_nick, user_age, user_gender, user_photo, user_about_me, user_preferences,
#                user_city, us_index))
# conn.commit()


def all_id_csv():
    """
    Функция записи id пользователей в csv файл
    игнорировать предупреждение для подключения, отличного от SQLAlchemy
    смотрите: github.com/pandas-dev/pandas/issues/45660
    """
    file = pd.read_sql('SELECT user_name FROM users', conn)
    file.to_csv('users.csv', index=False)
    return 'user_name клиентов успешно записаны в файл csv'


# print(all_id_csv())

def reviews(user_name: str, description: str):
    """
    Функция для записи текста отзывов и предложений
    :param user_name: имя пользователя
    :param description: текст отзыва и/или предложения
    :return: Информация в таблицу отзывов и предложений внесена
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO reviews_and_suggestions(user_name, description)
            VALUES (%s, %s);""", (user_name, description))
        return 'Информация в таблицу отзывов и предложений внесена'


us_name = 'Марина'
user_description = 'Ничего так приложение'
# print(reviews(us_name, user_description))
conn.commit()


def liked(user_name: str, liked_user: str):
    """
    Функция для записи пары (пользователь, выполняющий запрос-пользователь, который понравился) в таблицу user_liked
    :param user_name: пользователь, выполняющий запрос
    :param liked_user: пользователь, который понравился
    :return: Понравившийся пользователь добавлен (Такая пара уже существует)
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT user_name, liked_user FROM user_liked Where user_name = %s AND liked_user = %s""",
                    (user_name, liked_user))
        cur.fetchone()
        if cur.fetchone() is None:
            try:
                cur.execute("""
                       INSERT INTO user_liked(user_name, liked_user)
                       VALUES (%s, %s);""", (user_name, liked_user))
            except UniqueViolation:
                return 'Такая пара уже существует'
        return 'Понравившийся пользователь добавлен'


us_name = 'Настя'
like_user = 'Миша'
# print(liked(us_name, like_user))
conn.commit()


def not_liked(user_name: str, not_liked_user: str):
    """
    Функция для записи пары (пользователь, выполняющий запрос-пользователь, который не понравился) в таблицу blacklist
    :param user_name: пользователь, выполняющий запрос
    :param not_liked_user: пользователь, который не понравился
    :return: Пользователь, которые не нравится добавлен (Такая пара уже существует)
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT user_name, not_liked_user FROM blacklist Where user_name = %s AND not_liked_user = %s""",
                    (user_name, not_liked_user))
        cur.fetchone()
        if cur.fetchone() is None:
            try:
                cur.execute("""
                       INSERT INTO blacklist(user_name, not_liked_user)
                       VALUES (%s, %s);""", (user_name, not_liked_user))
            except UniqueViolation:
                return 'Такая пара уже существует'
        return 'Пользователь, которые не нравится добавлен'


us_name = 'Один'
not_like_us = 'Миша'
# print(not_liked(us_name, not_like_us))
conn.commit()


def delete_user(user_name: str):
    """
    Функкция для удаления пользователя из таблицы users (каскадом удаляет всю информацию по данному пользователю из всех аблиц)
    :param user_name: пользователь из таблицы users
    :return: пользователь удалён
    """
    with conn.cursor() as cur:
        cur.execute("""
                    DELETE FROM users WHERE user_name = %s;""", (user_name,))
        return 'Пользователь удалён из базы данных'


us_name = 'Катя'
# print(delete_user(us_name))
conn.commit()


#

def search(user_name):
    """
    Функция запроса вывода пола, возраста и предпочтений пользователя
    :param user_name: имя пользователя
    :return: список со словарём (пол, возрвст, предпочтения рльзователя, ведущего поиск)
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT gender, age, preferences FROM users WHERE user_name = %s""", (user_name,))
        res = cur.fetchall()
        res_list = []
        for row in res:
            res_list.append(dict(row))
        return res_list


us_name = 'Локи'


# pprint(search(us_name))


def search3(age: int, preferences: str, gender: str):
    """
    Функция запроса по выводу анкеты подходящих по параматрам пользователей (имя пользователя, ник, пол, информация о себе,
    его предпочтения, город, фото)
    :param age: возраст искомого кандидата
    :param preferences: пол искомого кандидата
    :param gender: предпочтения искомого кандидата
    :return: список словарей с подходящими кандтдатами
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT user_name, nick_name, gender, age, about_me, preferences, city, photo FROM users WHERE age BETWEEN (%s - 4) AND (%s + 4)
            AND gender = %s AND preferences = %s""", (age, age, preferences, gender))
        res = cur.fetchall()
        res_list = []
        for row in res:
            res_list.append(dict(row))
        return res_list


user_gender = 'мужчина'
user_age = 34
user_preferences = 'девушка'


# pprint(search3(user_age, user_preferences, user_gender))


def search_user(user_name):
    """
    Функция, которая проверяет по user_name есть ли такой пользователь в базе
    :param user_name: имя пользователя
    :return: имя пользователя либо None
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT user_name FROM users Where user_name = %s""", (user_name,))
        return cur.fetchone()


us_name = 'Валя'


# print(search_user(us_name))


def update_user_data(nick_name, age: int, gender: str, photo, about_me: str, preferences: str,
                     city: str, user_name: str):
    """

    :param nick_name: ник пользователя
    :param age: возраст пользователя
    :param gender: пол пользователя(мужчина, девушка, пара)
    :param photo: фото пользователя
    :param about_me: информатия пользователя о себе
    :param preferences: предпочтения пользователя(мужчина, девушка, пара)
    :param city: город, в котором проживает пользователь
    :param user_name: имя пользователя
    :return: Данные пользователя обновлены
    """
    with conn.cursor() as cur:
        cur.execute("""UPDATE users SET nick_name = %s, age = %s, gender = %s, photo = %s,
                    about_me = %s, preferences = %s, city = %s WHERE user_name = %s""",
                    (nick_name, age, gender, photo, about_me, preferences, city, user_name))
        return 'Данные пользователя обновлены'


us_name = 'Локи'
user_nick = '@lok'
user_age = 28
user_gender = 'мужчина'
user_photo = ''
user_about_me = 'нравится рисовать'
user_preferences = 'пара'
user_city = 'Рим'
print(
    update_user_data(user_nick, user_age, user_gender, user_photo, user_about_me, user_preferences, user_city, us_name))
conn.commit()

# def serch4():
#     with conn.cursor() as cur:
#         if cur.execute("""SELECT
#
# us_name = 'Локи'
# like_user = 'Настя'
# print(serch4())


#
# user_gender = 'мужчина'
# user_age = 32
# user_preferences = 'девушка'
# us_name = 'Локи'
#
#
# pprint(request(user_gender, user_age, user_preferences, us_name))


# def test(users_id: int, liked_id: int):
#     with conn.cursor() as cur:
#         cur.execute("""SELECT users_id, liked_id FROM user_liked WHERE users_id = %s AND liked_id = %s""",
#                     (users_id, liked_id))
#         cur.fetchone()
#         if cur.fetchone():
#             cur.execute("""SELECT users_id, liked_id FROM user_liked WHERE users_id = %s
#                 AND liked_id = %s AND users_id = %s AND liked_id = %s""", (liked_id, users_id))
#             return [users_id, liked_id]
#         # else:
#         #     return 'Совпадений нет'
#
#
# user_id = 1
# like_id = 2
# print(test(like_id, user_id))
