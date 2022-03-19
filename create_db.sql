CREATE TABLE servers(
 server_id INTEGER PRIMARY KEY AUTOINCREMENT,
 address text NOT NULL,
 status text NOT NULL
);

CREATE TABLE frames(
 frame_id INTEGER PRIMARY KEY AUTOINCREMENT,
 orig_frame_path text NOT NULL,
 upd_frame_path text,
 status text NOT NULL
);

CREATE TABLE processing_frames(
 id INTEGER PRIMARY KEY,
 frame_id INTEGER,
 server_id INTEGER,
 output_filename text,
 status text NOT NULL,
 FOREIGN KEY(frame_id) REFERENCES frames(frame_id),
 FOREIGN KEY(server_id) REFERENCES servers(server_id)
);


