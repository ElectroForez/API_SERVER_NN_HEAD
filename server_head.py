import time
from config_head import DB_PATH, VIDEO_PATH, PASSWORD
import sqlite3
import os
from DbManager import DbManager
from Video_nn import *
import requests
import traceback


class ServerHead:
    def __init__(self, video_path, db_path):
        self.video_path = video_path
        if not os.path.exists(self.video_path):
            os.mkdir(self.video_path)
        self.DbManager = DbManager(db_path)

    def save_self(self, method):
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)

        return wrapper

    @staticmethod
    def upload_frame(server_url, frame_path, **params):
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
        try:
            response = requests.request("POST", url, headers=headers, files=files, params=params)
        except requests.ConnectionError:
            return -1
        print(response.text)
        if response.status_code == 202:
            return response.json()['output_filename']
        else:
            return response.json(), response.status_code

    def start_work(self, videofile, upd_videofile='untitled.avi', *args_realsr):
        try:
            self.DbManager.prepare_db()
            if len(self.DbManager.get_avlb_servers()) == 0:
                print('All servers are not available')
                return -1
            return_code = improve_video(videofile, upd_videofile, *args_realsr,
                                        func_upscale=self.save_self(self.remote_processing))
            if return_code != 0:
                print('Error')
                return -1
        except sqlite3.Error as error:
            print("Ошибка при работе с sqlite:", error)
            print(traceback.format_exc())
        finally:
            if self.DbManager.sqlite_connection:
                self.DbManager.close_connection()
                print("Соединение с SQLite закрыто")

    def remote_processing(self, frames_path, upd_frames_path, *args_realsr):
        self.DbManager.add_frames(frames_path)
        params = {
            'realsr': ' '.join(args_realsr)
        }

        while True:
            frame_path = self.DbManager.get_waiting_frame()
            if frame_path is None:
                break
            if len(self.DbManager.get_avlb_servers()) == 0:
                print('All servers are down')
                return -1
            while True:
                time.sleep(0.5)
                # self.DbManager.watch_servers()
                server_url = self.DbManager.get_vacant_server()
                if server_url is not None:
                    break
            response = self.upload_frame(server_url, frame_path, **params)
            if response == -1:
                return -1
            elif type(response) == str:
                self.DbManager.add_proc_server(server_url, frame_path, response)
                print(f'Send {frame_path} to {server_url}')
            else:
                print(f'Send {frame_path} to {server_url}')
                print('Response:', response[0])
                print('Status code:', response[1])
                self.DbManager.update_status_serv(server_url)

        return 0


if __name__ == '__main__':
    server_head = ServerHead(VIDEO_PATH, DB_PATH)
    video_dir = VIDEO_PATH + 'barabans/'
    args_realsr = '-x -s 4'
    server_head.start_work(video_dir + 'upbar.mp4', video_dir, *args_realsr.split())
