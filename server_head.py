import time
from config_head import DB_PATH, API_PASSWORD, SERVERS_PATH, MAX_PARALLEL_UPLOAD, MAX_PARALLEL_DOWNLOAD
from DB_NAMES import ProcStatus, TableName
import sqlite3
import os
from DbManager import DbManager, uploading_control, downloading_control
from Video_nn import *
import requests
import traceback
import threading


class ServerHead:
    def __init__(self, db_path, servers_path, password):
        self.pass_header = {'X-PASSWORD': password}
        self.db_manager = DbManager(db_path, servers_path, password)
        self.smpho_dload = threading.BoundedSemaphore(MAX_PARALLEL_DOWNLOAD)
        self.smpho_upload = threading.BoundedSemaphore(MAX_PARALLEL_UPLOAD)

    def save_self(self, method):
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)
        return wrapper

    @uploading_control
    def upload_frame(self, proc_id, server_url, frame_path, **params):
        frames_path = frame_path.split('/')[-2]
        filename = frame_path.split('/')[-1]
        extension = filename.split('.')[-1]
        url = f'{server_url}/content/{frames_path}'

        files = [
            ('picture', (
                filename, open(frame_path, 'rb'),
                'image/' + extension))
        ]
        headers = self.pass_header
        try:
            response = requests.request("POST", url, headers=headers, files=files, params=params)
        except requests.ConnectionError:
            return -1
        print(f'Send {frame_path} to {server_url}')
        print(response.text)
        if response.status_code == 202:
            return response.json()['output_filename']
        else:
            return response.json(), response.status_code

    def download_frame(self, proc_id, server_url, output_path, **params):
        headers = self.pass_header
        try:
            response = requests.get(server_url, params=params, headers=headers)
            with open(output_path, 'wb') as f:
                f.write(response.content)
        except requests.ConnectionError:
            return -1

    def start_work(self, videofile, upd_videofile='untitled.avi', *args_realsr):
        try:
            self.db_manager.check_stuck()
            self.db_manager.prepare_db()
            if len(self.db_manager.get_avlb_servers()) == 0:
                print('All servers are not available')
                return -1
            return_code = improve_video(videofile, upd_videofile, *args_realsr,
                                        func_upscale=self.save_self(self.remote_processing))
            if return_code != 0:
                print('Error. End of work')
                return -1
        except sqlite3.Error as error:
            print("Ошибка при работе с sqlite:", error)
            print(traceback.format_exc())
        finally:
            if self.db_manager.sqlite_connection:
                self.db_manager.close_connection()
                print("Соединение с SQLite закрыто")

    def remote_processing(self, frames_path, upd_frames_path, *args_realsr):
        self.db_manager.add_frames(frames_path)
        output_frames_path = upd_frames_path.split('/')[-2] + '/'
        while True:
            frame_path = self.db_manager.get_waiting_frame()
            if frame_path is None:
                break
            while True:
                time.sleep(1)
                self.db_manager.watch_servers()
                if len(self.db_manager.get_avlb_servers()) == 0:
                    print('All servers are down')
                    return -1
                server_url = self.db_manager.get_vacant_server()
                if server_url is not None:
                    break
            output_name = frame_path.split('/')[-1].replace("jpg", "png")
            params = {
                'realsr': ' '.join(args_realsr),
                'output_name': output_name
            }
            proc_id = self.db_manager.add_proc_server(server_url, frame_path, output_frames_path + output_name)
            thread_upload = threading.Thread(target=self.upload_frame,
                                             args=(proc_id, server_url, frame_path),
                                             kwargs=params)
            thread_upload.start()

        return 0


if __name__ == '__main__':
    server_head = ServerHead(DB_PATH, SERVERS_PATH, API_PASSWORD)
    video_dir = 'videos/' + 'barabans/'
    args_realsr = ''
    server_head.start_work(video_dir + 'upbar.mp4', video_dir, *args_realsr.split())
