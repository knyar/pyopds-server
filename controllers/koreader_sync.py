"""KoReader sync storage and HTTP controller."""
import hashlib
import json
import os
import sqlite3
import base64
import time

KOREADER_SYNC_DB_PATH = os.environ.get('KOREADER_SYNC_DB_PATH', 'koreader_sync.db')
KOREADER_SYNC_LIBRARY_DIR = os.environ.get('LIBRARY_DIR', 'books')


def _is_truthy_env(value: str | None) -> bool:
    return (value or '').strip().lower() in ('1', 'true', 'yes', 'on')


KOREADER_SYNC_NORMALIZE = _is_truthy_env(os.environ.get('KOREADER_SYNC_NORMALIZE'))

# Byte offsets used by the partial-content md5. Matches the scheme used by some
# KoReader builds and other readers that hash sparse slices of the file rather
# than the whole content.
PARTIAL_HASH_OFFSETS = (
    0,
    1024,
    4096,
    16384,
    65536,
    262144,
    1048576,
    4194304,
    16777216,
    67108864,
    268435456,
    1073741824,
)
PARTIAL_HASH_CHUNK = 1024


class BookHashIndex:
    """Lazy SQLite-backed index of EPUB filename and partial-content md5 hashes.

    Used by the sync API in normalize mode to map any of the hashes a client
    may report (filename md5 or partial-content md5) to a single canonical
    document id, so different readers sync to the same record.
    """

    def __init__(self, db_path: str = KOREADER_SYNC_DB_PATH, library_dir: str = KOREADER_SYNC_LIBRARY_DIR):
        self.db_path = db_path
        self.library_dir = library_dir
        self._ensure_table()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_table(self):
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS epub_hashes (
                    relative_path TEXT PRIMARY KEY,
                    filename_md5 TEXT NOT NULL,
                    content_md5 TEXT NOT NULL,
                    mtime REAL NOT NULL,
                    size INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_epub_hashes_filename ON epub_hashes(filename_md5)'
            )
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_epub_hashes_content ON epub_hashes(content_md5)'
            )

    @staticmethod
    def compute_filename_md5(path: str) -> str:
        return hashlib.md5(os.path.basename(path).encode('utf-8')).hexdigest()

    @staticmethod
    def compute_content_md5(path: str) -> str:
        h = hashlib.md5()
        size = os.path.getsize(path)
        with open(path, 'rb') as f:
            for offset in PARTIAL_HASH_OFFSETS:
                if offset >= size:
                    break
                f.seek(offset)
                h.update(f.read(min(PARTIAL_HASH_CHUNK, size - offset)))
        return h.hexdigest()

    def _ensure_hashes(self, path: str) -> tuple[str | None, str | None]:
        """Return (filename_md5, content_md5) for path, refreshing the cache if stale."""
        try:
            stat = os.stat(path)
        except OSError:
            return None, None
        relative_path = os.path.relpath(path, self.library_dir)
        mtime = stat.st_mtime
        size = stat.st_size

        with self._get_connection() as conn:
            row = conn.execute(
                'SELECT filename_md5, content_md5, mtime, size FROM epub_hashes WHERE relative_path = ?',
                (relative_path,),
            ).fetchone()
        if row and row['mtime'] == mtime and row['size'] == size:
            return row['filename_md5'], row['content_md5']

        try:
            filename_md5 = self.compute_filename_md5(path)
            content_md5 = self.compute_content_md5(path)
        except OSError:
            return None, None

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO epub_hashes
                    (relative_path, filename_md5, content_md5, mtime, size)
                VALUES (?, ?, ?, ?, ?)
                """,
                (relative_path, filename_md5, content_md5, mtime, size),
            )
        return filename_md5, content_md5

    def find_canonical_id(self, hash_value: str) -> str | None:
        """Resolve a client-supplied hash to the canonical document id.

        The canonical id is the partial-content md5 of the matched epub. This
        means clients that hash by filename and clients that hash by content
        end up writing to the same sync record.

        Returns None if no epub in the library matches the given hash.
        """
        if not hash_value:
            return None

        with self._get_connection() as conn:
            cached_rows = conn.execute(
                """
                SELECT relative_path FROM epub_hashes
                WHERE filename_md5 = ? OR content_md5 = ?
                """,
                (hash_value, hash_value),
            ).fetchall()

        for row in cached_rows:
            full_path = os.path.join(self.library_dir, row['relative_path'])
            filename_md5, content_md5 = self._ensure_hashes(full_path)
            if filename_md5 == hash_value or content_md5 == hash_value:
                return content_md5

        if not os.path.isdir(self.library_dir):
            return None

        for root, _, files in os.walk(self.library_dir):
            for filename in files:
                if not filename.endswith('.epub'):
                    continue
                path = os.path.join(root, filename)
                filename_md5, content_md5 = self._ensure_hashes(path)
                if filename_md5 == hash_value or content_md5 == hash_value:
                    return content_md5
        return None

class KoReaderSyncStorage:
    def _ensure_user_table(self):
        with self._get_connection() as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password_md5 TEXT NOT NULL
                )
                '''
            )

    def create_user(self, username, password_md5):
        self._ensure_user_table()
        try:
            with self._get_connection() as conn:
                conn.execute(
                    'INSERT INTO users (username, password_md5) VALUES (?, ?)',
                    (username, password_md5)
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def verify_user(self, username, password_md5):
        self._ensure_user_table()
        with self._get_connection() as conn:
            row = conn.execute(
                'SELECT 1 FROM users WHERE username = ? AND password_md5 = ?',
                (username, password_md5)
            ).fetchone()
        return row is not None

    """SQLite-backed storage for KoReader sync progress."""

    def __init__(self, db_path=KOREADER_SYNC_DB_PATH):
        self.db_path = db_path
        self._ensure_tables()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_tables(self):
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_records (
                    user TEXT NOT NULL,
                    document TEXT NOT NULL,
                    percentage REAL,
                    progress TEXT,
                    device TEXT,
                    device_id TEXT,
                    timestamp REAL,
                    PRIMARY KEY (user, document)
                )
                """
            )

    def upsert_record(self, user, document, percentage, progress, device, device_id, timestamp):
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO sync_records (user, document, percentage, progress, device, device_id, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user, document, percentage, progress, device, device_id, timestamp),
            )

    def fetch_records(self, user, document=None):
        if document:
            query = "SELECT * FROM sync_records WHERE user = ? AND document = ?"
            params = (user, document)
        else:
            query = "SELECT * FROM sync_records WHERE user = ?"
            params = (user,)
        query += " ORDER BY timestamp ASC"
        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


class KoReaderSyncController:
    ERROR_NO_DATABASE = 1000
    ERROR_INTERNAL = 2000
    ERROR_UNAUTHORIZED_USER = 2001
    ERROR_USER_EXISTS = 2002
    ERROR_INVALID_FIELDS = 2003
    ERROR_DOCUMENT_FIELD_MISSING = 2004
    ERROR_DOCUMENT_NOT_FOUND = 2005

    _sync_storage_instance = None
    _book_hash_index_instance = None

    def __init__(self, request_handler):
        if KoReaderSyncController._sync_storage_instance is None:
            KoReaderSyncController._sync_storage_instance = KoReaderSyncStorage()
        if KOREADER_SYNC_NORMALIZE and KoReaderSyncController._book_hash_index_instance is None:
            KoReaderSyncController._book_hash_index_instance = BookHashIndex(
                KOREADER_SYNC_DB_PATH, KOREADER_SYNC_LIBRARY_DIR
            )
        self.request = request_handler
        self.sync_storage = KoReaderSyncController._sync_storage_instance
        self.book_hash_index = (
            KoReaderSyncController._book_hash_index_instance
            if KOREADER_SYNC_NORMALIZE
            else None
        )

    def _resolve_document(self, document):
        """In normalize mode, resolve a client hash to a canonical document id.

        Returns the canonical id on success, or None if the request was rejected
        (in which case an error response has already been sent).
        """
        if self.book_hash_index is None:
            return document
        canonical = self.book_hash_index.find_canonical_id(document)
        if not canonical:
            self._send_json_error(
                self.ERROR_DOCUMENT_NOT_FOUND,
                'No epub in the library matches the provided document hash',
            )
            return None
        return canonical

    def _is_valid_field(self, field):
        """Check if field is a non-empty string."""
        return isinstance(field, str) and len(field) > 0

    def _is_valid_key_field(self, field):
        """Check if field is a non-empty string without colons."""
        return self._is_valid_field(field) and ":" not in field

    def register(self):
        payload = self._parse_json_body()
        if payload is None:
            return

        username = payload.get('username')
        password_md5 = payload.get('password')
        if not self._is_valid_key_field(username) or not self._is_valid_field(password_md5):
            self._send_json_error(self.ERROR_INVALID_FIELDS, 'Invalid username or password')
            return

        if self.sync_storage.create_user(username, password_md5):
            print("User created:", username)
            self._send_json_response({'username': username}, status=201)
        else:
            self._send_json_error(self.ERROR_USER_EXISTS, 'User already exists')

    def login(self):
        user = self.request.headers.get('X-Auth-User')
        password_md5 = self.request.headers.get('X-Auth-Key')
        if not self._is_valid_key_field(user) or not self._is_valid_field(password_md5):
            self._send_json_error(self.ERROR_INVALID_FIELDS, 'Invalid X-Auth-User or X-Auth-Key')
            return

        if self.sync_storage.verify_user(user, password_md5):
            self._send_json_response({'authorized': "OK"})
        else:
            self._send_json_error(self.ERROR_UNAUTHORIZED_USER, 'Unauthorized: invalid user or password')

    def get_sync_records(self):
        user = self._authorize()
        document = self.request.path.split('syncs/progress/')[1]

        if not user:
            self._send_json_error(self.ERROR_UNAUTHORIZED_USER, 'Unauthorized: invalid user or password')
            return

        if not document:
            self._send_json_error(self.ERROR_DOCUMENT_FIELD_MISSING, 'Missing document parameter')
            return

        if not self._is_valid_key_field(document):
            self._send_json_error(self.ERROR_DOCUMENT_FIELD_MISSING, 'Invalid document parameter')
            return

        canonical = self._resolve_document(document)
        if canonical is None:
            return

        records = self.sync_storage.fetch_records(
            user=user,
            document=canonical,
        )

        if not records:
            self._send_json_response({})
            return

        row = records[0]
        res = {}
        if row['percentage'] is not None:
            res['percentage'] = row['percentage']
        if row['progress'] is not None:
            res['progress'] = row['progress']
        if row['device'] is not None:
            res['device'] = row['device']
        if row['device_id'] is not None:
            res['device_id'] = row['device_id']
        if row['timestamp'] is not None:
            res['timestamp'] = row['timestamp']
        if res:
            res['document'] = document if self.book_hash_index is None else canonical

        self._send_json_response(res)

    def store_sync_records(self):
        user = self._authorize()
        if not user:
            self._send_json_error(self.ERROR_UNAUTHORIZED_USER, 'Unauthorized: invalid user or password')
            return

        payload = self._parse_json_body()

        if payload is None:
            return

        document = payload.get('document')
        percentage_str = payload.get('percentage')
        progress = payload.get('progress')
        device = payload.get('device')
        device_id = payload.get('device_id')

        if not self._is_valid_key_field(document) or not self._is_valid_field(progress) or not self._is_valid_field(device):
            self._send_json_error(self.ERROR_INVALID_FIELDS, 'Invalid payload: document, progress, and device required')
            return

        try:
            percentage = float(percentage_str)
        except (TypeError, ValueError):
            self._send_json_error(self.ERROR_INVALID_FIELDS, 'Invalid percentage')
            return

        canonical = self._resolve_document(document)
        if canonical is None:
            return

        timestamp = time.time()

        self.sync_storage.upsert_record(user, canonical, percentage, progress, device, device_id, timestamp)

        self._send_json_response({
            'document': canonical,
            'timestamp': timestamp,
        })

    def _authorize(self):
        """Authorize user using X-Auth-User and X-Auth-Key headers."""
        user = self.request.headers.get('X-Auth-User')
        password_md5 = self.request.headers.get('X-Auth-Key')
        if self._is_valid_key_field(user) and self._is_valid_field(password_md5):
            if self.sync_storage.verify_user(user, password_md5):
                return user
        return None

    def _parse_json_body(self):
        """Parse JSON request body."""
        content_length = self.request.headers.get('Content-Length')
        if content_length is None:
            self._send_json_error(self.ERROR_INVALID_FIELDS, 'Missing Content-Length header')
            return None

        try:
            length = int(content_length)
        except ValueError:
            self._send_json_error(self.ERROR_INVALID_FIELDS, 'Invalid Content-Length header')
            return None

        if length <= 0:
            self._send_json_error(self.ERROR_INVALID_FIELDS, 'Empty request body')
            return None

        body = self.request.rfile.read(length)

        try:
            return json.loads(body.decode('utf-8'))
        except json.JSONDecodeError:
            self._send_json_error(self.ERROR_INVALID_FIELDS, 'Invalid JSON payload')
            return None

    def _send_json_response(self, data, status=200):
        """Send JSON response."""
        payload = json.dumps(data, ensure_ascii=False).encode('utf-8')
        self.request.send_response(status)
        self.request.send_header('Content-Type', 'application/json')
        self.request.send_header('Content-Length', str(len(payload)))
        self.request.end_headers()
        self.request.wfile.write(payload)

    def _send_json_error(self, code, message):
        """Send JSON error response with custom code."""
        # Mapping explicite des codes d'erreur vers les codes HTTP standards
        http_status_map = {
            self.ERROR_NO_DATABASE: 500,
            self.ERROR_INTERNAL: 500,
            self.ERROR_UNAUTHORIZED_USER: 401,
            self.ERROR_USER_EXISTS: 409,
            self.ERROR_INVALID_FIELDS: 400,
            self.ERROR_DOCUMENT_FIELD_MISSING: 400,
            self.ERROR_DOCUMENT_NOT_FOUND: 404,
        }
        http_status = http_status_map.get(code, 500)
        
        response = {
            'status': 'error',
            'code': code,
            'error': message,
        }
        self._send_json_response(response, status=http_status)

    def _extract_basic_auth(self, parsed_url=None):
        """Extract username and password from Authorization: Basic header."""
        auth_header = self.request.headers.get('Authorization')
        if auth_header and auth_header.lower().startswith('basic '):
            try:
                b64 = auth_header.split(' ', 1)[1].strip()
                decoded = base64.b64decode(b64).decode('utf-8')
                username, password_md5 = decoded.split(':', 1)
                return username, password_md5
            except Exception:
                return None, None
        return None, None


__all__ = [
    'KOREADER_SYNC_DB_PATH',
    'KOREADER_SYNC_LIBRARY_DIR',
    'KOREADER_SYNC_NORMALIZE',
    'BookHashIndex',
    'KoReaderSyncController',
    'KoReaderSyncStorage',
]

