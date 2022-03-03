from config_head import DB_PATH, VIDEO_PATH, PASSWORD
import requests
import sqlite3
import os
class ServerHead:
    def __init__(self, video_path, db_path):
        self.video_path = video_path
        self.db_path = db_path
        self.check()

    def check(self):
        if not os.path.exists(self.video_path):
            os.mkdir(self.video_path)
        if not os.path.exists(self.db_path):
            try:
                sqlite_connection = sqlite3.connect(self.db_path)
                cursor = sqlite_connection.cursor()
                with open('create_db.sql', 'r') as sqlite_file:
                    sql_script = sqlite_file.read()
                cursor.executescript(sql_script)
                print("База данных создана и успешно подключена к SQLite")
            except sqlite3.Error as error:
                print('Ошибка при подключению к sqlite', error)


if __name__ == '__main__':
    server_head = ServerHead(VIDEO_PATH, DB_PATH)