# tables
class TableName:
    SERVERS = 'servers'
    FRAMES = 'frames'
    PROCESSING = 'processing_frames'
    ALL_TABLES = [SERVERS, FRAMES, PROCESSING]


class ServerStatus:
    """possible server statuses"""

    BUSY = 'is_busy'
    VACANT = 'vacant'
    NOT_AVAILABLE = 'not available'


class FrameStatus:
    """possible frame statuses"""
    WAITING = 'is waiting'
    PROCESSING = 'is processing'
    UPDATED = 'updated'
    BAD_FRAME = 'bad frame'


class ProcessingStatus:
    """possible processing statuses"""
    LAUNCHED = 'launched'
    FINISHED = 'finished'
    LOST = 'lost'
    FAILED = 'failed'
    UPLOADING = 'uploading'
    DOWNLOADING = 'downloading'
