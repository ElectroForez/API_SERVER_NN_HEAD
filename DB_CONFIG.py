# tables
servers_table = 'servers'
frames_table = 'frames'
frames_servers_table = 'processing_frames'
tables = [servers_table, frames_table, frames_servers_table]
# CONST
# for servers
BUSY = 'is_busy'
VACANT = 'vacant'
NOT_AVAILABLE = 'not available'
# for frames
WAITING = 'is waiting'
PROCESSING = 'is processing'
UPDATED = 'updated'
BAD_FRAME = 'bad frame'
# for processing_frames
LAUNCHED = 'launched'
FINISHED = 'finished'
LOST = 'lost'
FAILED = 'failed'
UPLOADING = 'uploading'
DOWNLOADING = 'downloading'
