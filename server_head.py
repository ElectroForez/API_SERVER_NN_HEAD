import time
from config_head import DB_PATH, VIDEO_PATH, PASSWORD
import sqlite3
import os
from DbManager import DbManager
from Video_nn import *
import requests


class ServerHead:
    def __init__(self, video_path, db_path):
        self.video_path = video_path
        if not os.path.exists(self.video_path):
            os.mkdir(self.video_path)

        # self.db_path = db_path
        self.DbManager = DbManager(db_path)
    def save_self(self, method):
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)

        return wrapper

    def upload_frame(self, server_url, frame_path, **params):
        frames_path = frame_path.split('/')[-2]
        filename = frame_path.split('/')[-1]
        extension = filename.split('.')[-1]
        url = f'{server_url}/content/{frames_path}'

        files = [
            ('picture', (
                filename, open(frame_path, 'rb'),
                'image/' + extension))
        ]
        headers = {
            'X-PASSWORD': PASSWORD
        }

        response = requests.request("POST", url, headers=headers, files=files, params=params)
        print(response.text)
        if response.status_code == 202:
            return response.json()['output_filename']

    def start_work(self, videofile, upd_videofile='untitled.avi', *args_realsr):
        try:
            frame = self.DbManager.get_waiting_frame()
            print(frame)
            self.DbManager.check_db()
            self.DbManager.prepare_db()
            # return_code = improve_video(videofile, upd_videofile, *args_realsr,
            # #                             func_upscale=self.save_self(self.remote_processing))
            # if return_code != 0:
            #     print('Error')
            #     return
            time.sleep(60)

        except sqlite3.Error as error:
            print("Ошибка при работе с sqlite:", error)
        finally:
            if self.DbManager.sqlite_connection:
                self.DbManager.close_connection()
                print("Соединение с SQLite закрыто")

    def remote_processing(self, frames_path, upd_frames_path, *args_realsr):
        self.DbManager.add_frames(frames_path)
        params = {
            'output_filename': 'HelloApi.png',
            'realsr': args_realsr
        }

        while True:
            frame = self.DbManager.get_waiting_frame()
            if frame is None:
                break

            while True:
                self.DbManager.watch_servers()
                server = self.DbManager.get_vacant_server()
                if server is not None:
                    break

        return 0


if __name__ == '__main__':
    server_head = ServerHead(VIDEO_PATH, DB_PATH)
    video_dir = VIDEO_PATH + 'barabans/'
    server_head.start_work(video_dir + 'upbar.mp4', upd_videofile=video_dir)
