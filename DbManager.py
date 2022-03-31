import time

from config_head import MAX_UPLOAD_TIME, MAX_DOWNLOAD_TIME, MAX_PROCESSING_TIME
from DB_NAMES import *
import sqlite3
import os
import glob
import requests
from datetime import datetime


def uploading_control(upload_func):
    def wrapper(*args, **kwargs):
        self = args[0]
        self.smpho_upload.acquire()
        self_man = self.db_manager
        proc_id = args[1]
        try:
            new_manager = DbManager(self_man.db_path, self_man.servers_path, self_man.pass_header['X-PASSWORD'])
            new_manager.update_status(TableName.PROCESSING, ProcStatus.UPLOADING, proc_id)
            if upload_func(*args, **kwargs) == -1:
                new_manager.update_status(TableName.PROCESSING, ProcStatus.FAILED, proc_id)
            else:
                new_manager.update_status(TableName.PROCESSING, ProcStatus.LAUNCHED, proc_id)
        except sqlite3.Error as e:
            print('with upload sqlite3 error', e)
        finally:
            new_manager.close_connection()
            self.smpho_upload.release()
    return wrapper


def downloading_control(download_func):
    def wrapper(*args, **kwargs):
        self = args[0]
        self.smpho_dload.acquire()
        self_man = self.db_manager
        proc_id = args[1]
        try:
            new_manager = DbManager(self_man.db_path, self_man.servers_path, self_man.pass_header['X-PASSWORD'])
            new_manager.update_status(TableName.PROCESSING, ProcStatus.UPLOADING, proc_id)
            if download_func(*args, **kwargs) == -1:
                new_manager.update_status(TableName.PROCESSING, ProcStatus.LOST, proc_id)
            else:
                new_manager.update_status(TableName.PROCESSING, ProcStatus.FINISHED, proc_id)
        except sqlite3.Error as e:
            print('with download sqlite3 error', e)
        finally:
            new_manager.close_connection()
            self.smpho_dload.release()
    return wrapper


class DbManager:
    def __init__(self, db_path, servers_path, password):
        self.db_path = db_path
        self.check_db()
        self.sqlite_connection = sqlite3.connect(self.db_path)
        self.cursor = self.sqlite_connection.cursor()
        self.pass_header = {'X-PASSWORD': password}
        self.servers_path = servers_path

    def check_db(self):
        db_exists = os.path.exists(self.db_path)
        sqlite_connection = sqlite3.connect(self.db_path)
        cursor = sqlite_connection.cursor()
        if not db_exists:
            create_db_file = 'create_db.sql'
            with open(create_db_file, 'r') as sqlite_file:
                sql_script = sqlite_file.read()
            cursor.executescript(sql_script)
        for table in TableName.ALL_TABLES:
            cursor.execute(f'SELECT * FROM {table} LIMIT 1')
        cursor.close()
        sqlite_connection.close()
        return 0

    def clear_db(self):
        for table in TableName.ALL_TABLES:
            self.cursor.execute(f'DELETE FROM {table}')
            self.cursor.execute(f'UPDATE SQLITE_SEQUENCE SET seq = 0 WHERE name = "{table}"')
        self.sqlite_connection.commit()

    def prepare_db(self):
        self.clear_db()
        with open(self.servers_path, 'r') as file:
            servers = [server.strip() for server in file.readlines()]
        for server in servers:
            self.add_server(server)

    def add_server(self, address):
        server_status = self.get_status_serv(address)
        if server_status == ServerStatus.NOT_AVAILABLE:
            print(address, 'not available')
        self.cursor.execute(f'INSERT INTO {TableName.SERVERS}(address, status) VALUES ("{address}", "{server_status}")')

    def get_status_serv(self, address):
        url = address + r'/check/busy'
        try:
            response = requests.get(url, headers=self.pass_header)
            if response.status_code == 200:
                is_busy = response.json()['status']
                if is_busy:
                    return ServerStatus.BUSY
                else:
                    return ServerStatus.VACANT
        except requests.ConnectionError as error:
            print('get_status_serv error:', error)
            return ServerStatus.NOT_AVAILABLE

    def add_frames(self, frames_path):
        frames = glob.glob(frames_path + '*.*')
        frames = [el for el in frames if el.endswith(('.jpg', '.png', '.webp'))]
        for frame in frames:
            self.cursor.execute(
                f'INSERT INTO {TableName.FRAMES}(orig_frame_path, status) VALUES ("{frame}", "{FrameStatus.WAITING}")')
        self.sqlite_connection.commit()

    def get_vacant_server(self):
        return self.select(f'SELECT address FROM {TableName.SERVERS}'
                           f' WHERE status="{ServerStatus.VACANT}" LIMIT 1', 1)

    def get_waiting_frame(self):
        return self.select(f'SELECT orig_frame_path FROM {TableName.FRAMES}'
                           f' WHERE status="{FrameStatus.WAITING}" LIMIT 1', 1)

    def get_servers(self):
        return self.cursor.execute(f'SELECT * FROM {TableName.SERVERS}')

    def update_status(self, table, status, cond_id):  # manual update
        fields_id = {TableName.SERVERS: "server_id", TableName.FRAMES: "frame_id", TableName.PROCESSING: "proc_id"}
        field_id = fields_id[table]
        self.cursor.execute(f'UPDATE {table} '
                            f'SET status="{status}", '
                            f'upd_status_time="{datetime.now()}" '
                            f'WHERE {field_id} = {cond_id}')
        self.sqlite_connection.commit()

    def update_status_serv(self, address):  # automatic update
        self.cursor.execute(f'UPDATE {TableName.SERVERS} '
                            f'SET status={self.get_status_serv(address)}, '
                            f'upd_status_time="{datetime.now()}" '
                            f'WHERE server_id = {self.get_id_server(address)}')
        self.sqlite_connection.commit()

    def watch_servers(self):
        servers = self.get_servers()
        for server_id, server_url, old_status, _ in servers:
            cur_status = self.get_status_serv(server_url)
            if cur_status != old_status:
                self.update_status(TableName.SERVERS, cur_status, server_id)
        self.sqlite_connection.commit()

    def check_stuck(self):
        time_constraints = {
            ProcStatus.UPLOADING: MAX_UPLOAD_TIME,
            ProcStatus.LAUNCHED: MAX_PROCESSING_TIME,
            ProcStatus.DOWNLOADING: MAX_DOWNLOAD_TIME
        }
        query = ""
        for status, time_constraint in time_constraints.items():
            query += f'''SELECT proc_id, frame_id, server_id FROM {TableName.PROCESSING} pf
        WHERE (strftime("%s", 'now') - strftime("%s", pf.upd_status_time)) > {time_constraint} AND status="{status}"
        UNION
        '''
        query = query[0:query.rfind('UNION')]
        stuck_proc = self.select(query)
        for proc_id, frame_id, server_id in stuck_proc:
            self.update_status(TableName.SERVERS, ServerStatus.NOT_AVAILABLE, server_id)
            self.update_status(TableName.FRAMES, FrameStatus.WAITING, frame_id)
            self.update_status(TableName.PROCESSING, ProcStatus.LOST, proc_id)

    def check_uploads(self):
        query = f"""SELECT pf.proc_id, f.orig_frame_path, s.address FROM {TableName.PROCESSING} pf
        JOIN {TableName.FRAMES} f ON f.frame_id = pf.frame_id
        JOIN {TableName.SERVERS} s ON s.server_id = pf.server_id
        WHERE pf.status="{ProcStatus.UPLOADING}";
        """
        for proc_id, frame_path, address in self.select(query):
            address += '/info/order'
            try:
                response = requests.get(address, headers=self.pass_header)
                if response.status_code == 200:
                    order = response.json()['Files list']
                    for file in order:
                        if frame_path.endswith(file):
                            self.update_status(TableName.PROCESSING, ProcStatus.LAUNCHED, proc_id)
                            break
            except requests.ConnectionError:
                self.update_status_serv(address)

    def check_updated(self):
        query = f'''SELECT pf.output_filename, s.address FROM {TableName.PROCESSING} pf
        JOIN servers s ON s.server_id = pf.server_id
        WHERE pf.status = '{ProcStatus.LAUNCHED}';
        '''
        launched_frames = self.select(query)
        for output_filename, address in launched_frames:
            url = address + '/content/' + output_filename
            try:
                response = requests.get(url, params=self.pass_header)
                if response.status_code != 404:
                    pass
            except requests.ConnectionError:
                self.update_status_serv(address)

    def add_proc_server(self, server_url, frame_path, output_filename):
        server_id = self.get_id_server(server_url)
        frame_id = self.get_id_frame(frame_path)
        self.update_status(TableName.SERVERS, ServerStatus.VACANT, server_id)
        self.update_status(TableName.FRAMES, FrameStatus.PROCESSING, frame_id)
        self.cursor.execute(
            f'INSERT INTO {TableName.PROCESSING} '
            f'(frame_id, server_id, output_filename, status) VALUES'
            f'("{frame_id}", "{server_id}", "{output_filename}", "{ProcStatus.IN_ORDER}")'
        )
        self.sqlite_connection.commit()
        return self.get_id_proc(frame_path, server_url)

    def get_id_server(self, server_url):
        return self.select(f'SELECT server_id FROM {TableName.SERVERS} WHERE address = "{server_url}"', 1)

    def get_id_frame(self, frame_path):
        return self.select(f'SELECT frame_id FROM {TableName.FRAMES} WHERE orig_frame_path = "{frame_path}"', 1)

    def get_avlb_servers(self):
        return self.select(f'SELECT * FROM {TableName.SERVERS} WHERE status != "{ServerStatus.NOT_AVAILABLE}"')

    def get_id_proc(self, frame, server, actual=True):
        if type(frame) == int:
            frame_id = frame
        else:
            frame_id = self.get_id_frame(frame)
        if type(server) == int:
            server_id = server
        else:
            server_id = self.get_id_server(server)
        if None in (server_id, frame_id):
            raise sqlite3.DataError("Uncorrect ids")

        query = f'SELECT proc_id FROM {TableName.PROCESSING} ' \
                f'WHERE frame_id = {frame_id} AND server_id = {server_id}'
        if actual:
            ACTUAL_STATUS = [ProcStatus.UPLOADING, ProcStatus.LAUNCHED, ProcStatus.DOWNLOADING, ProcStatus.IN_ORDER]
            ACTUAL_STATUS = [f'"{status}"' for status in ACTUAL_STATUS]
            return self.select(query + f" AND status IN ({', '.join(ACTUAL_STATUS)})", 1)
        else:
            return self.select(query)

    def select(self, query, fetch_size=-1):
        self.cursor.execute(query)
        if fetch_size == -1:
            result = self.cursor.fetchall()
        elif fetch_size == 1:
            result = self.cursor.fetchone()
            if result is not None:
                result = result[0]
        else:
            result = self.cursor.fetchmany(fetch_size)
        return result

    def close_connection(self):
        self.cursor.close()
        self.sqlite_connection.commit()
        self.sqlite_connection.close()
