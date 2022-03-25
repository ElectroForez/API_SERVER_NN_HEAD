from config_head import  MAX_UPLOAD_TIME, MAX_DOWNLOAD_TIME, MAX_PROCESSING_TIME
from DB_NAMES import *
import sqlite3
import os
import glob
import requests
from datetime import datetime


class DbManager:
    def __init__(self, db_path, servers_path, password):
        self.db_path = db_path
        self.check_db()
        self.sqlite_connection = sqlite3.connect(self.db_path)
        self.cursor = self.sqlite_connection.cursor()
        self.pass_param = {'X-PASSWORD': password}
        self.servers_path = servers_path

    def check_db(self):
        print('Start checking DB')
        db_exists = os.path.exists(self.db_path)
        sqlite_connection = sqlite3.connect(self.db_path)
        cursor = sqlite_connection.cursor()
        if not db_exists:
            create_db_file = 'create_db.sql'
            with open(create_db_file, 'r') as sqlite_file:
                sql_script = sqlite_file.read()
            cursor.executescript(sql_script)
        for table in tables:
            cursor.execute(f'SELECT * FROM {table} LIMIT 1')
        cursor.close()
        sqlite_connection.close()
        print('End checking DB')
        return 0

    def clear_db(self):
        for table in tables:
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
        if server_status == NOT_AVAILABLE:
            print(address, 'not available')
        self.cursor.execute(f'INSERT INTO {servers_table}(address, status) VALUES ("{address}", "{server_status}")')

    def get_status_serv(self, address):
        url = address + r'/check/busy'
        try:
            response = requests.get(url, headers=self.pass_param)
            if response.status_code == 200:
                is_busy = response.json()['status']
                if is_busy:
                    return BUSY
                else:
                    return VACANT
        except requests.ConnectionError as error:
            print('get_status_serv error:', error)
            return NOT_AVAILABLE

    def add_frames(self, frames_path):
        frames = glob.glob(frames_path + '*.*')
        frames = [el for el in frames if el.endswith(('.jpg', '.png', '.webp'))]
        for frame in frames:
            self.cursor.execute(
                f'INSERT INTO {frames_table}(orig_frame_path, status) VALUES ("{frame}", "{WAITING}")')
        self.sqlite_connection.commit()

    def get_vacant_server(self):
        return self.select(f'SELECT address FROM {servers_table} WHERE status="{VACANT}" LIMIT 1', False)

    def get_waiting_frame(self):
        return self.select(f'SELECT orig_frame_path FROM {frames_table} WHERE status="{WAITING}" LIMIT 1', False)

    def get_servers(self):
        return self.cursor.execute(f'SELECT * FROM {servers_table}')

    def update_status(self, table, status, cond_id):  # manual update
        fields_id = {servers_table: "server_id", frames_table: "frame_id", frames_servers_table: "proc_id"}
        field_id = fields_id[table]
        self.cursor.execute(f'UPDATE {table} '
                            f'SET status="{status}", '
                            f'upd_status_time="{datetime.now()}" '
                            f'WHERE {field_id} = {cond_id}')
        self.sqlite_connection.commit()

    def update_status_serv(self, address):  # automathic update
        self.cursor.execute(f'UPDATE {servers_table} '
                            f'SET status={self.get_status_serv(address)}, '
                            f'upd_status_time="{datetime.now()}" '
                            f'WHERE server_id = {self.get_id_server(address)}')
        self.sqlite_connection.commit()

    def watch_servers(self):
        servers = self.get_servers()
        for server_id, server_url, old_status, _ in servers:
            cur_status = self.get_status_serv(server_url)
            if cur_status != old_status:
                self.update_status(servers_table, cur_status, server_id)
        self.sqlite_connection.commit()

    def check_stuck(self):
        time_constraints = {
            UPLOADING: MAX_UPLOAD_TIME,
            LAUNCHED: MAX_PROCESSING_TIME,
            DOWNLOADING: MAX_DOWNLOAD_TIME
        }
        query = ""
        for status, time_constraint in time_constraints.items():
            query += f'''SELECT proc_id, frame_id, server_id FROM {frames_servers_table} pf
        WHERE (strftime("%s", 'now') - strftime("%s", pf.upd_status_time)) > {time_constraint} AND status="{status}"
        UNION
        '''
        query = query[0:query.rfind('UNION')]
        stuck_proc = self.select(query)
        for proc_id, frame_id, server_id in stuck_proc:
            self.update_status(servers_table, NOT_AVAILABLE, server_id)
            self.update_status(frames_table, WAITING, frame_id)
            self.update_status(frames_servers_table, LOST, proc_id)

    def check_uploads(self):
        query = f"""SELECT pf.proc_id, f.orig_frame_path, s.address FROM {frames_servers_table} pf
        JOIN {frames_table} f ON f.frame_id = pf.frame_id
        JOIN {servers_table} s ON s.server_id = pf.server_id
        WHERE pf.status="{UPLOADING}";
        """
        for proc_id, frame_path, address in self.select(query):
            address += '/info/order'
            try:
                response = requests.get(address, headers=self.pass_param)
                if response.status_code == 200:
                    order = response.json()['Files list']
                    for file in order:
                        if frame_path.endswith(file):
                            self.update_status(frames_servers_table, LAUNCHED, proc_id)
                            break
            except requests.ConnectionError:
                self.update_status_serv(address)

    # def check_updated(self):
    #     query = f'''SELECT pf.output_filename, s.address FROM {frames_servers_table} pf
    #     JOIN servers s ON s.server_id = pf.server_id
    #     WHERE pf.status = '{LAUNCHED}';
    #     '''
    #     launched_frames = self.select(query)
    #     for output_filename, address in launched_frames:
    #         url = address + '/content/' + output_filename
    #         try:
    #             response = requests.get(url, params=self.pass_param)
    #             if response.status_code != 404:
    #                 pass
    #         except requests.ConnectionError:
    #             self.update_status_serv(address)

    def add_proc_server(self, server_url, frame_path, output_filename):
        server_id = self.get_id_server(server_url)
        frame_id = self.get_id_frame(frame_path)
        self.update_status(servers_table, BUSY, server_id)
        self.update_status(frames_table, PROCESSING, frame_id)
        self.cursor.execute(
            f'INSERT INTO {frames_servers_table} '
            f'(frame_id, server_id, output_filename, status) VALUES' 
            f'("{frame_id}", "{server_id}", "{output_filename}", "{UPLOADING}")'
        )
        self.sqlite_connection.commit()

    def get_id_server(self, server_url):
        return self.select(f'SELECT server_id FROM {servers_table} WHERE address = "{server_url}"', False)

    def get_id_frame(self, frame_path):
        return self.select(f'SELECT frame_id FROM {frames_table} WHERE orig_frame_path = "{frame_path}"', False)

    def get_avlb_servers(self):
        return self.select(f'SELECT * FROM {servers_table} WHERE status != "{NOT_AVAILABLE}"')

    def select(self, query, fetch_all=True):
        self.cursor.execute(query)
        if fetch_all:
            result = self.cursor.fetchall()
        else:
            result = self.cursor.fetchone()
            if result is not None:
                result = result[0]
        return result

    def close_connection(self):
        self.cursor.close()
        self.sqlite_connection.commit()
        self.sqlite_connection.close()
