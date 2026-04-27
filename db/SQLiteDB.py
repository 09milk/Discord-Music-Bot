from exceptions.DJExceptions import DJDBException
# from const.config import dynamodb_table, dynamodb_hist_table
from const.options import default_init_vol
from const.SongInfo import SongInfo
from const.DBFields import SongAttr, HistAttr
from const.helper import error_log, error_log_e, get_time, vid_to_thumbnail, chop_query
from DJDBInterface import DJDBInterface



class SQLiteDB(DJDInterface):
    pass


