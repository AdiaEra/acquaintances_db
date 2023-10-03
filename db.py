import psycopg2

with psycopg2.connect(database="acquaintances_db", user="postgres", password="") as conn:
    with conn.cursor() as cur:
        def delete_db():
            """
            Функция, удаляющая таблицы базы данных
            :return: БД удалена
            """
            cur.execute("""
            DROP TABLE blacklist;
            DROP TABLE user_liked;
            DROP TABLE reviews_and_suggestions;
            DROP TABLE users
            CASCADE;
            """)
            return 'БД удалена'

        # print(delete_db())
        conn.commit()

        def create_db():
            """
            Функция, создающая структуру БД (таблицы)
            :return: База данных создана
            """
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users(
                    users_id BIGINT PRIMARY KEY,
                    name VARCHAR(40) NOT NULL,
                    nick_name VARCHAR(40),
                    age SMALLINT NOT NULL,
                    gender VARCHAR(10) NOT NULL,
                    photo BYTEA,
                    about_me TEXT NOT NULL,
                    preferences VARCHAR(10),
                    city VARCHAR(30) NOT NULL
                );
                CREATE TABLE IF NOT EXISTS blacklist(
                    users_id BIGINT REFERENCES users(users_id)
                    ON DELETE CASCADE,
                    not_liked_id BIGINT NOT NULL,
                    UNIQUE (users_id, not_liked_id)
                );
                CREATE TABLE IF NOT EXISTS reviews_and_suggestions(
                    users_id BIGINT REFERENCES users(users_id)
                    ON DELETE CASCADE,
                    description TEXT
                );
                CREATE TABLE IF NOT EXISTS user_liked(
                    users_id BIGINT REFERENCES users(users_id)
                    ON DELETE CASCADE,
                    liked_id BIGINT NOT NULL,
                    UNIQUE (users_id, liked_id)
                );
                """)
            return 'База данных создана'

        # print(create_db())
        conn.commit()
