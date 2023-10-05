from pprint import pprint

import psycopg2.extras
from psycopg2.errors import UniqueViolation
import pandas as pd

from db import conn


def add_user(users_id: int, name: str, nick_name, age: int, gender: str, photo, about_me: str, preferences: str,
             city: str):
    """
    Функция, позволяющая добавить нового пользователя в таблицу users
    :param users_id: id пользователя
    :param name: имя пользователя
    :param nick_name: ник пользователя
    :param age: возраст пользователя
    :param gender: пол пользователя (мужчина, девушка, пара)
    :param photo: фото пользователя
    :param about_me: информатия пользователя о себе
    :param preferences: предпочтения пользователя (мужчина, девушка, пара)
    :param city: город, в котором проживает пользователь
    :return: Новый пользователь добавлен (Пользователь с таким id уже есть в базе)
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT users_id FROM users Where users_id = %s""", (users_id,))
        cur.fetchone()
        if cur.fetchone() is None:
            try:
                cur.execute("""
                            INSERT INTO users (users_id, name, nick_name, age, gender, photo, about_me, preferences, city)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                            (users_id, name, nick_name, age, gender, photo, about_me, preferences, city))
            except UniqueViolation:
                return 'Пользователь с таким id уже есть в базе'
        return 'Новый пользователь добавлен'


user_id = 11113
user_name = 'Миша'
user_nick = 'ma@'
user_age = 31
user_gender = 'мужчина'
user_photo = ''
user_about_me = 'нравится слушать музыку'
user_preferences = 'девушка'
user_city = 'Калуга'

# print(add_user(user_id, user_name, user_nick, user_age, user_gender, user_photo, user_about_me, user_preferences,
#                user_city))
conn.commit()


def all_id_csv():
    """
    Функция записи id пользователей в csv файл
    игнорировать предупреждение для подключения, отличного от SQLAlchemy
    смотрите: github.com/pandas-dev/pandas/issues/45660
    """
    file = pd.read_sql('SELECT users_id FROM users', conn)
    file.to_csv('users.csv', index=False)
    return 'id клиентов успешно записаны в файл csv'


# print(all_id_csv())

def reviews(users_id: int, description: str):
    """
    Функция для записи текста отзывов и предложений
    :param users_id: id пользователя
    :param description: текст отзыва и/или предложения
    :return: Информация в таблицу отзывов и предложений внесена
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO reviews_and_suggestions(users_id, description)
            VALUES (%s, %s);""", (users_id, description))
        return 'Информация в таблицу отзывов и предложений внесена'


user_id = 111
user_description = 'Ничего так приложение'

# print(reviews(user_id, user_description))
conn.commit()


def liked(users_id: int, liked_id: int):
    """
    Функция для записи пары (пользователь, выполняющий запрос-пользователь, который понравился) в таблицу user_liked
    :param users_id: пользователь, выполняющий запрос
    :param liked_id: пользователь, который понравился
    :return: Понравившийся пользователь добавлен (Такая пара уже существует)
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT users_id, liked_id FROM user_liked Where users_id = %s AND liked_id = %s""",
                    (users_id, liked_id))
        cur.fetchone()
        if cur.fetchone() is None:
            try:
                cur.execute("""
                       INSERT INTO user_liked(users_id, liked_id)
                       VALUES (%s, %s);""", (users_id, liked_id))
            except UniqueViolation:
                return 'Такая пара уже существует'
        return 'Понравившийся пользователь добавлен'


user_id = 111
like_id = 44444444444444

# print(liked(user_id, like_id))
conn.commit()


def not_liked(users_id: int, not_liked_id: int):
    """
    Функция для записи пары (пользователь, выполняющий запрос-пользователь, который не понравился) в таблицу blacklist
    :param users_id: пользователь, выполняющий запрос
    :param not_liked_id: пользователь, который не понравился
    :return: Пользователь, которые не нравится добавлен (Такая пара уже существует)
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT users_id, not_liked_id FROM blacklist Where users_id = %s AND not_liked_id = %s""",
                    (users_id, not_liked_id))
        cur.fetchone()
        if cur.fetchone() is None:
            try:
                cur.execute("""
                       INSERT INTO blacklist(users_id, not_liked_id)
                       VALUES (%s, %s);""", (users_id, not_liked_id))
            except UniqueViolation:
                return 'Такая пара уже существует'
        return 'Пользователь, которые не нравится добавлен'


user_id = 111
not_like_id = 111111111111111

# print(not_liked(user_id, not_like_id))
conn.commit()


def delete_user(users_id: int):
    """
    Функкция для удаления пользователя из таблицы users (каскадом удаляет всю информацию по данному пользователю из всех аблиц)
    :param users_id: пользователь из таблицы users
    :return: id пользователя удалён
    """
    with conn.cursor() as cur:
        cur.execute("""
                    DELETE FROM users WHERE users_id = %s;""", (users_id,))
        return 'Пользователь удалён из базы данных'


user_id = 333333333

# print(delete_user(user_id))
conn.commit()


def request(gender: str, age: int, preferences: str, name: str):
    """
Функция запроса вывода ника, пола, возраста, города, фото и предпочтений пользователя
:param name:
:param gender: пол искомого пользователя (мужчина, девушка, пара)
:param age: возраст искомого пользователя
:param preferences: предпочтения искомого пользователя (мужчина, девушка, пара)
:return: список словарей подходящих пользователей (ник, возраст, информация о себе, город, фото)
"""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""SELECT name FROM users Where name = %s""", (name,))
        cur.fetchone()
        if cur.fetchone():
            cur.execute(
                """SELECT nick_name, gender, age, about_me, city, photo FROM users WHERE gender = %s AND age = %s AND preferences = %s AND name = %s""",
                (gender, age, preferences, name))
            res = cur.fetchall()
            res_list = []
            for row in res:
                res_list.append(dict(row))
            return res_list
        else:
            return 'Подходящего варианта нет'


user_gender = 'девушка'
user_age = 31
user_preferences = 'мужчина'
user_name = 'Adia'


# pprint(request(user_gender, user_age, user_preferences, user_name))


def search(name):
    """
    Функция запроса вывода ника, пола, возраста и предпочтений понравившегося пользователя
    :param name:
    :return:
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT gender, age, preferences, city FROM users WHERE name = %s""", (name,))
        res = cur.fetchall()
        res_list = []
        for row in res:
            res_list.append(dict(row))
        return res_list


user_name = 'Adia'

# pprint(search(user_name))


def search3(age: int, preferences: str, gender: str):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT nick_name, gender, age, about_me, city FROM users WHERE age BETWEEN (%s - 4) AND (%s + 4) 
            AND gender = %s AND preferences = %s""", (age, age, preferences, gender))
        res = cur.fetchall()
        res_list = []
        for row in res:
            res_list.append(dict(row))
        return res_list


user_gender = 'девушка'
user_age = 30
user_preferences = 'мужчина'

pprint(search3(user_age, user_preferences, user_gender))
