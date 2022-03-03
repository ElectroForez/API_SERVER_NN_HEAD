CREATE TABLE servers(
 host_id INTEGER PRIMARY KEY AUTOINCREMENT,
 adress text,
 status text
);

CREATE TABLE frames(
 frame_id INTEGER PRIMARY KEY AUTOINCREMENT,
 frame_path text,
 status text
);
