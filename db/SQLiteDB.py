import json
import os
import sqlite3

from exceptions.DJExceptions import DJDBException
from const.config import sqlite_db_path
from const.options import default_init_vol
from const.SongInfo import SongInfo
from const.DBFields import SongAttr, HistAttr
from const.helper import error_log, error_log_e, get_time, vid_to_thumbnail, chop_query
from db.DJDBInterface import DJDBInterface


class SQLiteDB(DJDBInterface):
    def __init__(self) -> None:
        self.db_path = sqlite_db_path
        self.conn = None
        self.table_name = 'songs'
        self.hist_table_name = 'history'

    def connect(self):
        db_exists = os.path.exists(self.db_path)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        if not db_exists:
            self._create_tables()

    def _load_schema(self):
        schema = 'SQLiteSchema.sql'
        with open(schema, 'r', encoding='utf-8') as schema_file:
            return schema_file.read()

    def _create_tables(self):
        cursor = self.conn.cursor()
        schema = self._load_schema()
        cursor.executescript(schema)
        self.conn.commit()

    def _row_to_item(self, row):
        if row is None:
            return None
        item = dict(row)
        if SongAttr.Queries in item and item[SongAttr.Queries] is not None:
            try:
                item[SongAttr.Queries] = json.loads(item[SongAttr.Queries])
            except Exception:
                item[SongAttr.Queries] = []
        return item

    def _serialize_queries(self, queries):
        return json.dumps(queries if queries is not None else [])

    def _db_update(self, vID, attr, val):
        cursor = self.conn.cursor()
        cursor.execute(
            f'UPDATE {self.table_name} SET {attr} = ? WHERE vID = ?',
            (val, vID)
        )
        self.conn.commit()

    def db_get(self, vID, get_attrs=None) -> SongInfo:
        if self.conn is None:
            raise DJDBException('SQLite connection not initialized')

        select_attrs = get_attrs[:] if get_attrs else SongAttr.get_all()
        if SongAttr.vID not in select_attrs:
            select_attrs.append(SongAttr.vID)
        if SongAttr.Title not in select_attrs:
            select_attrs.append(SongAttr.Title)
        if SongAttr.ChannelID not in select_attrs:
            select_attrs.append(SongAttr.ChannelID)

        cols = ', '.join(select_attrs)
        cursor = self.conn.cursor()
        cursor.execute(f'SELECT {cols} FROM {self.table_name} WHERE vID = ?', (vID,))
        row = cursor.fetchone()
        if not row:
            raise DJDBException(f'No item for vID: {vID}')
        item = self._row_to_item(row)
        return super().dbItemToSongInfo(item)

    def add_query(self, query, songInfo, song_exist=False):
        if type(songInfo) != SongInfo:
            songInfo = SongInfo(songInfo, '', '')
            song_exist = True

        vID = songInfo.vID
        if not song_exist:
            song, inserted = self.insert_song(songInfo, query=query)
            if not inserted:
                song = self.db_get(vID, [SongAttr.Queries])
        else:
            song = self.db_get(vID, [SongAttr.Queries])

        query_words = chop_query(query.lower())
        existing_queries = song.get(SongAttr.Queries) or []
        for q in existing_queries:
            if q == query_words:
                return

        existing_queries.append(query_words)
        self._db_update(vID, SongAttr.Queries, self._serialize_queries(existing_queries))

    def insert_song(self, songInfo, qcount=0, songVol=default_init_vol, newDJable=True, query=None):
        existing = self.find_song_match(songInfo.vID)
        if existing:
            return existing, False

        item = songInfo.dictify_info()
        item[SongAttr.STitle] = item[SongAttr.Title].lower()
        item[SongAttr.Queries] = [] if query is None else [[chop_query(query.lower())]]
        item[SongAttr.DJable] = 1 if newDJable else 0
        item[SongAttr.SongVol] = int(songVol * 100)
        item[SongAttr.Duration] = 0
        item[SongAttr.Qcount] = qcount

        cursor = self.conn.cursor()
        cursor.execute(f'''
            INSERT INTO {self.table_name} (vID, Title, STitle, ChannelID, Queries, DJable, SongVol, Duration, Qcount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            item[SongAttr.vID],
            item[SongAttr.Title],
            item[SongAttr.STitle],
            item[SongAttr.ChannelID],
            self._serialize_queries(item[SongAttr.Queries]),
            item[SongAttr.DJable],
            item[SongAttr.SongVol],
            item[SongAttr.Duration],
            item[SongAttr.Qcount]
        ))
        self.conn.commit()
        return SQLiteDB.dbItemToSongInfo(item), True

    def remove_song(self, vid):
        cursor = self.conn.cursor()
        cursor.execute(f'DELETE FROM {self.table_name} WHERE vID = ?', (vid,))
        cursor.execute(f'DELETE FROM {self.hist_table_name} WHERE vID = ?', (vid,))
        self.conn.commit()

    def set_djable(self, vID, djable=True):
        self._db_update(vID, SongAttr.DJable, 1 if djable else 0)

    def update_duration(self, vID, duration):
        try:
            old_duration = self.db_get(vID, [SongAttr.Duration])[SongAttr.Duration]
        except DJDBException as e:
            error_log('Cannot update duration: ' + str(e))
            return

        if old_duration == 0 or old_duration != duration:
            self._db_update(vID, SongAttr.Duration, int(duration))

    def increment_qcount(self, vID):
        cursor = self.conn.cursor()
        cursor.execute(
            f'UPDATE {self.table_name} SET Qcount = Qcount + 1 WHERE vID = ?',
            (vID,)
        )
        self.conn.commit()

    def find_djable(self, vID) -> bool:
        try:
            return bool(self.db_get(vID, [SongAttr.DJable])[SongAttr.DJable])
        except DJDBException as e:
            error_log('cannot find djable: ' + str(e))
            return None

    def find_rand_song(self, dj=True):
        cursor = self.conn.cursor()
        if dj:
            cursor.execute(f'SELECT vID FROM {self.table_name} WHERE DJable = 1 ORDER BY RANDOM() LIMIT 1')
        else:
            cursor.execute(f'SELECT vID FROM {self.table_name} ORDER BY RANDOM() LIMIT 1')
        row = cursor.fetchone()
        return row['vID'] if row else None

    def find_rand_songs(self, n=10, dj=True):
        cursor = self.conn.cursor()
        if dj:
            cursor.execute(
                f'SELECT vID, Title, ChannelID, Duration, SongVol FROM {self.table_name} WHERE DJable = 1 ORDER BY RANDOM() LIMIT ?',
                (n,)
            )
        else:
            cursor.execute(
                f'SELECT vID, Title, ChannelID, Duration, SongVol FROM {self.table_name} ORDER BY RANDOM() LIMIT ?',
                (n,)
            )
        rows = cursor.fetchall()
        return [SQLiteDB.dbItemToSongInfo(self._row_to_item(row)) for row in rows]

    def find_query_match(self, query):
        cursor = self.conn.cursor()
        cursor.execute(f'SELECT vID, Queries FROM {self.table_name}')
        rows = cursor.fetchall()
        if not rows:
            return None

        query_words = chop_query(query.lower())
        for row in rows:
            item = self._row_to_item(row)
            for q in item.get(SongAttr.Queries, []):
                if q == query_words:
                    return self.db_get(item[SongAttr.vID])
        return None

    def list_all_songs(self, dj=None, top=10, needed_attr=None, return_song_type=list):
        cursor = self.conn.cursor()
        sql = f'SELECT * FROM {self.table_name}'
        params = []
        where = []
        if dj is not None:
            where.append('DJable = ?')
            params.append(1 if dj else 0)
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        if top is not None:
            sql += ' LIMIT ?'
            params.append(top)
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        if not rows:
            return None

        items = [self._row_to_item(row) for row in rows]
        if needed_attr is not None and len(needed_attr) > 0:
            if return_song_type == list:
                return [[item.get(a) for a in needed_attr] for item in items]
            return items
        return items

    def search(self, search_term, top=10):
        cursor = self.conn.cursor()
        term = f'%{search_term.lower()}%'
        cursor.execute(
            f'SELECT vID, Title, ChannelID FROM {self.table_name} WHERE STitle LIKE ? LIMIT ?',
            (term, top)
        )
        title_rows = cursor.fetchall()
        title_searched_vids = [row['vID'] for row in title_rows]
        title_searched_songs = [
            SongInfo(row['vID'], row['Title'], row['ChannelID'], vid_to_thumbnail(row['vID']))
            for row in title_rows
        ]

        cursor.execute(f'SELECT vID, Title, ChannelID, Queries FROM {self.table_name}')
        query_rows = cursor.fetchall()
        query_words = chop_query(search_term.lower())
        query_searched_songs = []
        for row in query_rows:
            if row['vID'] in title_searched_vids:
                continue
            item = self._row_to_item(row)
            for song_query in item.get(SongAttr.Queries, []):
                if any(word in song_query for word in query_words):
                    query_searched_songs.append(
                        SongInfo(
                            item[SongAttr.vID],
                            f"{item[SongAttr.Title]} [{'/'.join(song_query)}]",
                            item[SongAttr.ChannelID],
                            vid_to_thumbnail(item[SongAttr.vID])
                        )
                    )
                    break
        return title_searched_songs + query_searched_songs

    def add_history(self, vID, serverID, serverName, player):
        cursor = self.conn.cursor()
        cursor.execute(
            f'INSERT INTO {self.hist_table_name} (Time, vID, ServerID, ServerName, Player) VALUES (?, ?, ?, ?, ?)',
            (str(get_time()), vID, serverID, serverName, player)
        )
        self.conn.commit()

    def get_hist_rank(self, serverID=None, dj=False, top=20):
        cursor = self.conn.cursor()
        sql = f'SELECT vID, COUNT(*) as plays FROM {self.hist_table_name}'
        params = []
        where = []
        if serverID is not None:
            where.append('ServerID = ?')
            params.append(serverID)
        if dj:
            where.append('Player = ?')
            params.append('DJ')
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' GROUP BY vID ORDER BY plays DESC'
        if top is not None:
            sql += ' LIMIT ?'
            params.append(top)

        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        if not rows:
            return None

        ranked = []
        for row in rows:
            title = self.db_get(row['vID'], [SongAttr.Title])[SongAttr.Title]
            ranked.append((row['vID'], title, row['plays']))
        return ranked

    def get_hist_count(self, vID, serverID=None, dj=False):
        cursor = self.conn.cursor()
        sql = f'SELECT COUNT(*) as count FROM {self.hist_table_name} WHERE vID = ?'
        params = [vID]
        if serverID is not None:
            sql += ' AND ServerID = ?'
            params.append(serverID)
        if dj:
            sql += ' AND Player = ?'
            params.append('DJ')
        cursor.execute(sql, tuple(params))
        row = cursor.fetchone()
        return row['count'] if row else 0


if __name__ == '__main__':
    db = SQLiteDB()
    db.connect()
    print('SQLiteDB connected')


