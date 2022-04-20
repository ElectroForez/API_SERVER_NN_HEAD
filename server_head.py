from config_head import DB_PATH, API_PASSWORD, SERVERS_FILENAME, MAX_PARALLEL_UPLOAD, MAX_PARALLEL_DOWNLOAD
import sqlite3
from dbManager import DbManager, loading_control
import requests
import traceback
import threading
from datetime import datetime
import sys
import os
import argparse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from video_nn.video_nn import improve_video


class ServerHead:
    def __init__(self, db_path, servers_path, password):
        self.pass_header = {'X-PASSWORD': password}
        self.db_manager = DbManager(db_path, servers_path, password)
        self.smpho_dload = threading.BoundedSemaphore(MAX_PARALLEL_DOWNLOAD)  # semaphore for download
        self.smpho_upload = threading.BoundedSemaphore(MAX_PARALLEL_UPLOAD)  # semaphore for upload

    @loading_control
    def upload_frame(self, proc_id, server_url, frame_path, **params):
        """Uploading frame to server_url"""
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
        print(f'Send {frame_path} to {server_url} {datetime.now()}')
        print(response.text)
        if response.status_code == 202:
            return response.json()['output_filename']
        else:
            return -1

    @loading_control
    def download_frame(self, proc_id, server_url, output_path, **params):
        """Downloading frame from server_url"""
        headers = self.pass_header
        try:
            response = requests.get(server_url, params=params, headers=headers)
            print(f"Download {output_path} from {server_url} {datetime.now()}. "
                  f"Size = {response.headers['content-length']}")
            if response.status_code == 404:
                return -1
            with open(output_path, 'wb') as file:
                file.write(response.content)
        except requests.ConnectionError:
            return -1

    def start_work(self, videofile, upd_videofile='untitled.avi', *args_realsr):
        """enter function """
        try:
            self.db_manager.check_stuck()
            self.db_manager.clear_db()
            self.db_manager.add_servers()
            if len(self.db_manager.get_avlb_servers()) == 0:
                print('All servers are not available')
                return -1
            return_code = improve_video(videofile, upd_videofile, *args_realsr,
                                        func_upscale=self.remote_processing)
            if return_code != 0:
                print('Error. End of work')
                return -1
        except sqlite3.Error as error:
            print("Error while working with sqlite:", error)
            print(traceback.format_exc())
        finally:
            if self.db_manager.sqlite_connection:
                self.db_manager.close_connection()
                print("SQLite connection closed")
        print('Successful complete')

    def download_updates(self, output_frames_path):
        """Checks and download updated frames"""
        updated = self.db_manager.get_updated()
        for proc_id, frame_url in updated:
            frame_name = frame_url[frame_url.rfind('/') + 1:]
            thread_dload = threading.Thread(target=self.download_frame,
                                            args=(proc_id, frame_url),
                                            kwargs=({'output_path': output_frames_path + frame_name})
                                            )
            thread_dload.start()

    def remote_processing(self, frames_path, upd_frames_path, *args_realsr):
        """
        Manages and distributes frames from frames_path to servers that process them.
        Save results in upd_frames_path
        """
        self.db_manager.add_frames(frames_path)
        self.db_manager.add_upd_frames(upd_frames_path)
        output_frames_path = upd_frames_path.split('/')[-2] + '/'
        while True:
            if self.db_manager.is_all_processed():
                break
            frame_path = self.db_manager.get_waiting_frame()
            while True:
                self.db_manager.watch_servers()
                self.download_updates(upd_frames_path)
                if len(self.db_manager.get_avlb_servers()) == 0:
                    print('All servers are down')
                    return -1
                server_url = self.db_manager.get_vacant_server()
                if None not in (server_url, frame_path):
                    break
            output_name = self.db_manager.get_update_name(frame_path)
            output_path = output_frames_path + output_name
            params = {
                'realsr': ' '.join(args_realsr),
                'output_name': output_name
            }

            proc_id = self.db_manager.add_proc(server_url, frame_path, output_path)
            if self.db_manager.check_exists(server_url, output_path):
                print(f'{output_path} is already on the {server_url}')
                frame_url = server_url + '/content/' + output_path
                dload_path = upd_frames_path + output_name
                thread_dload = threading.Thread(target=self.download_frame,
                                                args=(proc_id, frame_url),
                                                kwargs=({'output_path': dload_path})
                                                )
                thread_dload.start()
            else:
                thread_upload = threading.Thread(target=self.upload_frame,
                                                 args=(proc_id, server_url, frame_path),
                                                 kwargs=params
                                                 )
                thread_upload.start()
        return 0


if __name__ == '__main__':
    if os.environ.get('IS_DOCKER'):
        mounted_path = '/mounted/'
        if not os.path.exists(mounted_path):
            raise IOError(f"{mounted_path} not found")
        servers_path = mounted_path + SERVERS_FILENAME
        video_dir = mounted_path
    else:
        servers_path = SERVERS_FILENAME
        video_dir = ''

    server_head = ServerHead(DB_PATH, servers_path, API_PASSWORD)
    parser = argparse.ArgumentParser(prog='Server API',
                                     description='Head for server nn. Improve video on remote servers')
    parser.add_argument('-i', '--input', type=str, help='Input path for video', required=True)
    parser.add_argument('-o', '--output', type=str, default='untitled.avi',
                        help='Output path for video. Temporary files will be stored in the same path.')
    parser.add_argument('-r', '--realsr', metavar='REALSR ARGS', default='', type=str)
    args = parser.parse_args()

    input_video = video_dir + args.input
    output_video = video_dir + args.output
    args_realsr = args.realsr
    server_head.start_work(input_video, output_video, *args_realsr.split())
