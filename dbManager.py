from config_head import MAX_UPLOAD_TIME, MAX_DOWNLOAD_TIME, MAX_PROCESSING_TIME, MAX_NOT_AVAILABLE
from DB_NAMES import *
import sqlite3
import os
import glob
import requests
from json import JSONDecodeError
from datetime import datetime


class DbManager:
    def __init__(self, db_path, servers_path, password):
        self.db_path = db_path
        self.check_db()
        self.sqlite_connection = sqlite3.connect(self.db_path)
        self.cursor = self.sqlite_connection.cursor()
        self.pass_header = {'X-PASSWORD': password}
        self.servers_path = servers_path

    def check_db(self):
        """checks table in db and create db if db bot exists"""
        db_exists = os.path.exists(self.db_path)
        sqlite_connection = sqlite3.connect(self.db_path)
        cursor = sqlite_connection.cursor()
        absScriptPath = os.path.abspath(__file__)
        path, filename = os.path.split(absScriptPath)
        if not db_exists:
            create_db_file = path + '/create_db.sql'
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

    def update_server_list(self):
        """synchronize servers file with db"""
        with open(self.servers_path, 'r') as file:
            file_servers = [server.strip() for server in file.readlines()]

        db_servers = self.select(f'SELECT address FROM {TableName.SERVERS}')

        if db_servers is not None:
            db_servers = [row[0] for row in db_servers]
        else:
            db_servers = []

        file_servers = set(file_servers)
        db_servers = set(db_servers)

        need_add = file_servers - db_servers
        need_delete = db_servers - file_servers

        for server_url in need_add:
            if server_url.endswith('/'):
                server_url = server_url[:-1]
            self.add_server(server_url)

        for server_url in need_delete:
            self.delete_server(server_url)
        return

    # def add_servers(self):
    #     """add servers to db"""
    #     with open(self.servers_path, 'r') as file:
    #         file_servers = [server.strip() for server in file.readlines()]
    #     for server_url in file_servers:
    #         if server_url.endswith('/'):
    #             server_url = server_url[:-1]
    #         self.add_server(server_url)

    def delete_server(self, address):
        actual_proc_id = self.get_id_proc_by_server(address)
        if actual_proc_id is not None:
            self.cancel_proc(actual_proc_id)
        self.cursor.execute(f'DELETE FROM servers WHERE server_id={self.get_id_server(address)}')
        self.sqlite_connection.commit()


    def add_server(self, address):
        """add server to db"""
        server_status = self.get_status_serv(address)
        if server_status == ServerStatus.NOT_AVAILABLE:
            print(address, 'not available')
        elif server_status == ServerStatus.INVALID_URL:
            print(address, 'invalid url')
            return
        elif server_status == ServerStatus.INVALID_PASS:
            print(ServerStatus.INVALID_PASS, address)
            return
        self.cursor.execute(f'INSERT INTO {TableName.SERVERS}(address, status) VALUES ("{address}", "{server_status}")')

    def get_status_serv(self, address):
        """returns actual status of the server"""
        check_url = address + r'/check/busy'
        db_status = None
        server_id = self.get_id_server(address)
        if server_id:
            db_status = self.select(f'SELECT status FROM {TableName.SERVERS} '
                                    f'WHERE server_id = {server_id}', 1)
        try:
            response = requests.get(check_url, headers=self.pass_header)
            if response.status_code == 200:
                is_busy = response.json()['status']
                if is_busy:
                    return ServerStatus.BUSY
                else:
                    if db_status and db_status == ServerStatus.RESERVED:
                        return ServerStatus.RESERVED
                    else:
                        return ServerStatus.VACANT
            elif response.status_code in (308, 404):
                return ServerStatus.NOT_AVAILABLE
            elif response.status_code == 401:
                return ServerStatus.INVALID_PASS
        except requests.ConnectionError:
            return ServerStatus.NOT_AVAILABLE
        except requests.exceptions.MissingSchema:
            return ServerStatus.INVALID_URL

    def add_frames(self, frames_path):
        """add frames to db"""
        frames = glob.glob(frames_path + '*.*')
        frames = list(filter(lambda el: el.endswith(('.jpg', '.png', '.webp')), frames))
        for frame in frames:
            self.cursor.execute(
                f'INSERT INTO {TableName.FRAMES} (orig_frame_path, status) VALUES ("{frame}", "{FrameStatus.WAITING}")')
        self.sqlite_connection.commit()
        print(f'Add to db {len(frames)} frames')

    def is_all_servers_broken(self):
        unbroken_servers = self.select('SELECT * FROM servers '
                                       f'WHERE status != "{ServerStatus.BROKEN}"')
        if len(unbroken_servers):
            return False
        else:
            return True

    def add_upd_frames(self, upd_frames_path):
        """add updated frames to db"""
        frames = self.select(f"select frame_id, orig_frame_path from {TableName.FRAMES}")
        upd_frames = glob.glob(upd_frames_path + '*.*')
        upd_frames = list(filter(lambda el: el.endswith(('.jpg', '.png', '.webp')), upd_frames))
        for frame_id, orig_frame_path in frames:
            upd_path = self.get_update_name(orig_frame_path, upd_frames_path)
            if upd_path in upd_frames:
                self.cursor.execute(f'UPDATE {TableName.FRAMES} SET '
                                    f'upd_frame_path = "{upd_path}",'
                                    f'status = "{FrameStatus.UPDATED}"'
                                    f'WHERE frame_id = {frame_id}')
                print(f'Already upd {orig_frame_path} on {upd_path}')
        self.sqlite_connection.commit()

    @staticmethod
    def get_update_name(frame_path, upd_frame_path=None):
        if upd_frame_path:
            return "/".join(upd_frame_path.split('/')[:-1]) + "/" + frame_path.split('/')[-1].replace("jpg", "png")
        else:
            return frame_path.split('/')[-1].replace("jpg", "png")

    def get_not_updated_frames(self):
        return self.select(f'SELECT orig_frame_path FROM {TableName.FRAMES}'
                           f' WHERE status != "{FrameStatus.UPDATED}"')

    def get_vacant_server(self):
        return self.select(f'SELECT address FROM {TableName.SERVERS}'
                           f' WHERE status="{ServerStatus.VACANT}" LIMIT 1', 1)

    def get_waiting_frame(self):
        return self.select(f'SELECT orig_frame_path FROM {TableName.FRAMES}'
                           f' WHERE status="{FrameStatus.WAITING}" LIMIT 1', 1)

    def get_servers(self):
        return self.select(f'SELECT * FROM {TableName.SERVERS}')

    def get_unbroken_servers(self):
        return self.select(f'SELECT * FROM {TableName.SERVERS} '
                           f'WHERE status != "{ServerStatus.BROKEN}"')

    def update_status(self, table, status, cond_id):
        """manual update status by id"""
        fields_id = {TableName.SERVERS: "server_id", TableName.FRAMES: "frame_id", TableName.PROCESSING: "proc_id"}
        field_id = fields_id[table]

        self.cursor.execute(f'UPDATE {table} '
                            f'SET status="{status}", '
                            f'upd_status_time="{datetime.now()}" '
                            f'WHERE {field_id} = {cond_id}')
        self.sqlite_connection.commit()

    def update_status_serv(self, address):
        """transmits the actual status of the server to the db"""
        self.cursor.execute(f'UPDATE {TableName.SERVERS} '
                            f'SET status="{self.get_status_serv(address)}", '
                            f'upd_status_time="{datetime.now()}" '
                            f'WHERE server_id = {self.get_id_server(address)}')
        self.sqlite_connection.commit()

    def watch_servers(self):
        """transmits the actual status of the all servers to the db"""
        servers = self.get_unbroken_servers()
        for server_id, server_url, old_status, _ in servers:
            cur_status = self.get_status_serv(server_url)
            if cur_status != old_status:
                self.update_status(TableName.SERVERS, cur_status, server_id)

    def check_stuck_proc(self):
        """checks stuck proc and restarts processing"""
        time_constraints = {
            ProcStatus.UPLOADING: MAX_UPLOAD_TIME,
            ProcStatus.LAUNCHED: MAX_PROCESSING_TIME,
            ProcStatus.DOWNLOADING: MAX_DOWNLOAD_TIME,
        }
        query_find_stuck = ""
        for status, time_constraint in time_constraints.items():
            query_find_stuck += f'''SELECT proc_id, frame_id, server_id FROM {TableName.PROCESSING} pf
        WHERE (strftime("%s", 'now', 'localtime') - strftime("%s", pf.upd_status_time)) > {time_constraint} AND status="{status}"
        UNION
        '''
        query_find_stuck = query_find_stuck[0:query_find_stuck.rfind('UNION')]
        stuck_proc = self.select(query_find_stuck)
        for proc_id, frame_id, server_id in stuck_proc:
            self.update_status(TableName.SERVERS, ServerStatus.NOT_AVAILABLE, server_id)
            self.update_status(TableName.FRAMES, FrameStatus.WAITING, frame_id)
            self.update_status(TableName.PROCESSING, ProcStatus.LOST, proc_id)

    def check_stuck_serv(self):
        """checks stuck servers and restarts processing"""
        query_find_stuck = f'''SELECT server_id FROM {TableName.SERVERS} s 
        WHERE (strftime("%s", 'now', 'localtime') - strftime("%s", s.upd_status_time)) > {MAX_NOT_AVAILABLE} 
        AND 
        status="{ServerStatus.NOT_AVAILABLE}"
        '''

        stuck_servers = self.select(query_find_stuck)

        for result_row in stuck_servers:
            server_id = result_row[0]
            self.update_status(TableName.SERVERS, ServerStatus.BROKEN, server_id)
            result = self.select('SELECT proc_id, frame_id FROM processing_frames pf '
                                 f'WHERE server_id = {server_id} '
                                 f'AND '
                                 f'status != "{ProcStatus.FINISHED}" '
                                 'ORDER BY upd_status_time DESC LIMIT 1')
            # proc_id = self.get_id_proc_by_server(server_id)
            # _, processed_frame_id = self.get_ids_server_frame(proc_id)
            if result:
                proc_id, processed_frame_id = result[0]  # result has a LIMIT 1
                self.update_status(TableName.FRAMES, FrameStatus.WAITING, processed_frame_id)
                self.update_status(TableName.PROCESSING, ProcStatus.LOST, proc_id)

    def cancel_proc(self, proc_id):
        server_id, frame_id = self.get_ids_server_frame(proc_id)
        self.update_status(TableName.SERVERS, ServerStatus.RECOVERING, server_id)
        self.update_status(TableName.FRAMES, FrameStatus.WAITING, frame_id)
        self.update_status(TableName.PROCESSING, ProcStatus.LOST, proc_id)

    def check_exists(self, address, frame_path):
        """checks the existence of this frame"""
        url = address + '/check/content/' + frame_path
        try:
            response = requests.get(url, headers=self.pass_header)
            if response.json()['File exists']:
                return True
            else:
                return False
        except requests.ConnectionError:
            pass
        except JSONDecodeError:
            pass

    def get_updated(self):
        """return list of proc_id and urls updated files"""
        query = f'''SELECT pf.proc_id, pf.output_filename, s.address FROM {TableName.PROCESSING} pf
        JOIN servers s ON s.server_id = pf.server_id
        WHERE pf.status = '{ProcStatus.LAUNCHED}';
        '''
        launched_frames = self.select(query)
        updated_files = []
        for proc_id, output_filename, address in launched_frames:
            if self.check_exists(address, output_filename):
                updated_files.append([proc_id, address + '/content/' + output_filename])
        return updated_files

    def add_proc(self, server_url, frame_path, output_filename):
        """add new processing"""
        server_id = self.get_id_server(server_url)
        frame_id = self.get_id_frame(frame_path)
        self.update_status(TableName.SERVERS, ServerStatus.RESERVED, server_id)
        self.update_status(TableName.FRAMES, FrameStatus.PROCESSING, frame_id)
        self.cursor.execute(
            f'INSERT INTO {TableName.PROCESSING} '
            f'(frame_id, server_id, output_filename, status) VALUES'
            f'("{frame_id}", "{server_id}", "{output_filename}", "{ProcStatus.IN_ORDER_UP}")'
        )
        self.sqlite_connection.commit()

    def add_download(self, proc_id):
        self.update_status(TableName.PROCESSING, ProcStatus.IN_ORDER_DN, proc_id)

    def get_ids_server_frame(self, proc_id):
        """return server_id, frame_id from processing by proc_id"""
        return self.select(f"SELECT server_id, frame_id FROM {TableName.PROCESSING} WHERE proc_id = {proc_id}")[0]

    def get_id_server(self, server_url):
        return self.select(f'SELECT server_id FROM {TableName.SERVERS} WHERE address = "{server_url}"', 1)

    def get_id_frame(self, frame_path):
        return self.select(f'SELECT frame_id FROM {TableName.FRAMES} WHERE orig_frame_path = "{frame_path}"', 1)

    def get_avlb_servers(self):
        """return available servers"""
        return self.select(f'SELECT * FROM {TableName.SERVERS} WHERE status != "{ServerStatus.NOT_AVAILABLE}"')

    def get_id_proc(self, frame, server, last=True):
        """
        get proc id by frame and server
        if actual give proc_id which is being processed now
        """
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
        if last:
            return self.select(query + " ORDER BY upd_status_time DESC LIMIT 1", 1)
        else:
            return self.select(query)

    def get_id_proc_by_server(self, server):
        if type(server) == int:
            server_id = server
        else:
            server_id = self.get_id_server(server)

        proc_id = self.select('SELECT proc_id FROM processing_frames pf '
                             f'WHERE server_id = {server_id} '
                             f'AND '
                             f'status != "{ProcStatus.FINISHED}" '
                             'ORDER BY upd_status_time DESC LIMIT 1', 1)
        return proc_id


    def select(self, query, fetch_size=-1):
        """return result of select query"""
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

    def after_download(self, proc_id, output_path):
        """for loading control"""
        server_id, frame_id = self.get_ids_server_frame(proc_id)
        self.cursor.execute(f'UPDATE {TableName.FRAMES} SET '
                            f'status = "{FrameStatus.UPDATED}",'
                            f'upd_frame_path = "{output_path}" '
                            f'WHERE frame_id = {frame_id}')

        self.cursor.execute(f'UPDATE {TableName.SERVERS} SET '
                            f'status = "{ServerStatus.RECOVERING}" '
                            f'WHERE server_id = {server_id}')

    def is_all_processed(self):
        not_processed = self.select(f"SELECT frame_id FROM {TableName.FRAMES} "
                                    f"WHERE status != '{FrameStatus.UPDATED}'")
        if len(not_processed):
            return False
        else:
            return True

    def get_progress(self):
        count_all_frames = self.select(f"SELECT COUNT(*) FROM {TableName.FRAMES}", 1)
        count_updated_frames = self.select(f'SELECT COUNT(*) FROM {TableName.FRAMES} '
                                           f'WHERE status = "{FrameStatus.UPDATED}"', 1)
        return {"all": count_all_frames, "updated": count_updated_frames}

    def print_progress(self):
        progress = self.get_progress()
        print(f"Completed {progress['updated']}/{progress['all']}")

    def close_connection(self):
        self.cursor.close()
        self.sqlite_connection.commit()
        self.sqlite_connection.close()


def loading_control(load_func):
    """decorator for loading. in db transmits the current load statuses"""

    def wrapper(*args, **kwargs):
        self = args[0]
        func_name = load_func.__name__
        if func_name.startswith('upload'):
            load = 'upload'
            status_before = ProcStatus.UPLOADING
            suc_status_aftr = ProcStatus.LAUNCHED
            bad_status_aftr = ProcStatus.FAILED
            smpho = self.smpho_upload
        elif func_name.startswith('download'):
            load = 'download'
            status_before = ProcStatus.DOWNLOADING
            suc_status_aftr = ProcStatus.FINISHED
            bad_status_aftr = ProcStatus.LOST
            smpho = self.smpho_dload
        else:
            raise Exception("Not a download/upload function")
        smpho.acquire()
        self_man = self.db_manager
        proc_id = args[1]
        new_manager = DbManager(self_man.db_path, self_man.servers_path, self_man.pass_header['X-PASSWORD'])
        server_id, frame_id = new_manager.get_ids_server_frame(proc_id)
        try:
            new_manager.update_status(TableName.PROCESSING, status_before, proc_id)
            if load_func(*args, **kwargs) != -1:
                new_manager.update_status(TableName.PROCESSING, suc_status_aftr, proc_id)
            else:
                new_manager.update_status(TableName.PROCESSING, bad_status_aftr, proc_id)
                if load == 'upload':
                    new_manager.update_status(TableName.FRAMES, FrameStatus.WAITING, frame_id)
            if load == 'download':
                new_manager.after_download(proc_id, kwargs['output_path'])
                new_manager.print_progress()
        except sqlite3.Error as e:
            print('with load function sqlite3 error', e)
        finally:
            new_manager.close_connection()
            smpho.release()

    return wrapper
