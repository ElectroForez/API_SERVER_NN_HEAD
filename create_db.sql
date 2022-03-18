CREATE TABLE servers(
 server_id INTEGER PRIMARY KEY AUTOINCREMENT,
 address text NOT NULL,
 status text
);

CREATE TABLE frames(
 frame_id INTEGER PRIMARY KEY AUTOINCREMENT,
 orig_frame_path text NOT NULL,
 upd_frame_path text,
 status text
);

CREATE TABLE proc_frames(
 id INTEGER PRIMARY KEY,
 frame_id INTEGER,
 server_id INTEGER,
 status text,
 FOREIGN KEY(frame_id) REFERENCES frames(frame_id),
 FOREIGN KEY(server_id) REFERENCES servers(server_id)
);


