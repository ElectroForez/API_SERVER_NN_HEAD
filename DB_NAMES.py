class TableName:
    SERVERS = 'servers'
    FRAMES = 'frames'
    PROCESSING = 'processing_frames'
    ALL_TABLES = [SERVERS, FRAMES, PROCESSING]  # list of names


class ServerStatus:
    """possible server statuses"""
    RESERVED = 'reserved'
    BUSY = 'is_busy'
    VACANT = 'vacant'
    NOT_AVAILABLE = 'not available'
    INVALID_URL = 'invalid url'
    INVALID_PASS = 'invalid pass'
    RECOVERING = 'recovering'
    BROKEN = 'broken'


class FrameStatus:
    """possible frame statuses"""
    WAITING = 'is waiting'
    PROCESSING = 'is processing'
    UPDATED = 'updated'
    BAD_FRAME = 'bad frame'


class ProcStatus:
    """possible processing statuses"""
    LAUNCHED = 'launched'
    FINISHED = 'finished'
    LOST = 'lost'
    FAILED = 'failed'
    UPLOADING = 'uploading'
    DOWNLOADING = 'downloading'
    IN_ORDER_DN = 'in dload order'
    IN_ORDER_UP = 'in upload order'
