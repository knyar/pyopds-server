"""Tests for the sync API normalization mode (BookHashIndex + controller).

These tests turn on `KOREADER_SYNC_NORMALIZE` and exercise the lazy hash index
that maps any of the hashes a client may report (filename md5 or partial
content md5) to a single canonical document id.
"""
import base64
import hashlib
import http.client
import importlib
import json
import os
import socketserver
import sys
import tempfile
import threading
import time
import unittest
import zipfile

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def create_epub(path: str, title: str, author: str, body_size: int = 0) -> None:
    """Write a minimal EPUB. `body_size` pads a chapter file to influence the content hash."""
    container_xml = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<container version='1.0' xmlns='urn:oasis:names:tc:opendocument:xmlns:container'>"
        "<rootfiles>"
        "<rootfile full-path='content.opf' media-type='application/oebps-package+xml'/>"
        "</rootfiles>"
        "</container>"
    )
    opf = (
        "<?xml version='1.0' encoding='UTF-8'?>"
        "<package xmlns='http://www.idpf.org/2007/opf' version='3.0'>"
        "<metadata xmlns:dc='http://purl.org/dc/elements/1.1/'>"
        f"<dc:title>{title}</dc:title>"
        f"<dc:creator>{author}</dc:creator>"
        "</metadata><manifest/><spine/>"
        "</package>"
    )
    base_dir = os.path.dirname(path)
    if base_dir and not os.path.exists(base_dir):
        os.makedirs(base_dir, exist_ok=True)
    with zipfile.ZipFile(path, 'w') as zf:
        zf.writestr('mimetype', 'application/epub+zip')
        zf.writestr('META-INF/container.xml', container_xml)
        zf.writestr('content.opf', opf)
        if body_size:
            # Padding so two epubs with otherwise-similar metadata produce
            # distinct partial-content hashes.
            zf.writestr('chapter.txt', ('x' * body_size).encode('ascii'))


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True


class BookHashIndexUnitTests(unittest.TestCase):
    """Direct unit tests for BookHashIndex without a running HTTP server."""

    @classmethod
    def setUpClass(cls):
        cls._orig_env = {
            'LIBRARY_DIR': os.environ.get('LIBRARY_DIR'),
            'KOREADER_SYNC_DB_PATH': os.environ.get('KOREADER_SYNC_DB_PATH'),
            'KOREADER_SYNC_NORMALIZE': os.environ.get('KOREADER_SYNC_NORMALIZE'),
        }
        cls.library_dir = tempfile.TemporaryDirectory()
        cls.db_file = tempfile.NamedTemporaryFile(delete=False)
        cls.db_file.close()
        os.environ['LIBRARY_DIR'] = cls.library_dir.name
        os.environ['KOREADER_SYNC_DB_PATH'] = cls.db_file.name
        os.environ['KOREADER_SYNC_NORMALIZE'] = '1'

        cls.module = importlib.reload(importlib.import_module('controllers.koreader_sync'))

        cls.alpha_path = os.path.join(cls.library_dir.name, 'alpha.epub')
        create_epub(cls.alpha_path, 'Alpha Title', 'Author One', body_size=10)
        cls.beta_path = os.path.join(cls.library_dir.name, 'sub', 'beta.epub')
        create_epub(cls.beta_path, 'Beta Title', 'Author Two', body_size=2048)

        cls.index = cls.module.BookHashIndex(cls.db_file.name, cls.library_dir.name)

    @classmethod
    def tearDownClass(cls):
        cls.library_dir.cleanup()
        if os.path.exists(cls.db_file.name):
            os.unlink(cls.db_file.name)
        for key, value in cls._orig_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(importlib.import_module('controllers.koreader_sync'))

    def test_filename_md5_matches_basename(self):
        expected = hashlib.md5(b'alpha.epub').hexdigest()
        self.assertEqual(self.module.BookHashIndex.compute_filename_md5(self.alpha_path), expected)

    def test_content_md5_is_deterministic(self):
        first = self.module.BookHashIndex.compute_content_md5(self.alpha_path)
        second = self.module.BookHashIndex.compute_content_md5(self.alpha_path)
        self.assertEqual(first, second)
        self.assertEqual(len(first), 32)

    def test_content_md5_differs_between_files(self):
        alpha = self.module.BookHashIndex.compute_content_md5(self.alpha_path)
        beta = self.module.BookHashIndex.compute_content_md5(self.beta_path)
        self.assertNotEqual(alpha, beta)

    def test_find_canonical_id_resolves_filename_hash(self):
        filename_md5 = self.module.BookHashIndex.compute_filename_md5(self.alpha_path)
        content_md5 = self.module.BookHashIndex.compute_content_md5(self.alpha_path)
        self.assertEqual(self.index.find_canonical_id(filename_md5), content_md5)

    def test_find_canonical_id_resolves_content_hash(self):
        content_md5 = self.module.BookHashIndex.compute_content_md5(self.beta_path)
        self.assertEqual(self.index.find_canonical_id(content_md5), content_md5)

    def test_find_canonical_id_unknown_hash_returns_none(self):
        self.assertIsNone(self.index.find_canonical_id('0' * 32))

    def test_find_canonical_id_empty_returns_none(self):
        self.assertIsNone(self.index.find_canonical_id(''))
        self.assertIsNone(self.index.find_canonical_id(None))

    def test_hashes_are_cached_and_refreshed_on_change(self):
        # First call populates the cache.
        before = self.index.find_canonical_id(
            self.module.BookHashIndex.compute_filename_md5(self.alpha_path)
        )
        # Mutate the file: the filename md5 stays the same but content md5
        # changes. Updating mtime/size invalidates the cache row.
        with zipfile.ZipFile(self.alpha_path, 'a') as zf:
            zf.writestr('extra.txt', b'cache-buster')
        # Force a different mtime so the cache invalidation kicks in even on
        # filesystems with coarse mtime granularity.
        new_time = time.time() + 5
        os.utime(self.alpha_path, (new_time, new_time))
        after = self.index.find_canonical_id(
            self.module.BookHashIndex.compute_filename_md5(self.alpha_path)
        )
        self.assertNotEqual(before, after)
        # The cache row for alpha.epub should now reflect the new content md5.
        self.assertEqual(
            after,
            self.module.BookHashIndex.compute_content_md5(self.alpha_path),
        )


class SyncNormalizeApiTests(unittest.TestCase):
    """End-to-end tests of the sync API in normalize mode."""

    @classmethod
    def setUpClass(cls):
        cls._orig_env = {
            'LIBRARY_DIR': os.environ.get('LIBRARY_DIR'),
            'KOREADER_SYNC_DB_PATH': os.environ.get('KOREADER_SYNC_DB_PATH'),
            'KOREADER_SYNC_NORMALIZE': os.environ.get('KOREADER_SYNC_NORMALIZE'),
        }
        cls.library_dir = tempfile.TemporaryDirectory()
        cls.db_file = tempfile.NamedTemporaryFile(delete=False)
        cls.db_file.close()
        os.environ['LIBRARY_DIR'] = cls.library_dir.name
        os.environ['KOREADER_SYNC_DB_PATH'] = cls.db_file.name
        os.environ['KOREADER_SYNC_NORMALIZE'] = '1'

        # Reload modules so module-level constants pick up the new env vars.
        cls.sync_module = importlib.reload(importlib.import_module('controllers.koreader_sync'))
        importlib.reload(importlib.import_module('controllers.opds'))
        importlib.reload(importlib.import_module('routes'))
        cls.server_mod = importlib.reload(importlib.import_module('server'))

        cls.alpha_path = os.path.join(cls.library_dir.name, 'alpha.epub')
        create_epub(cls.alpha_path, 'Alpha Title', 'Author One', body_size=10)
        cls.beta_path = os.path.join(cls.library_dir.name, 'sub', 'beta.epub')
        create_epub(cls.beta_path, 'Beta Title', 'Author Two', body_size=2048)

        cls.alpha_filename_md5 = cls.sync_module.BookHashIndex.compute_filename_md5(cls.alpha_path)
        cls.alpha_content_md5 = cls.sync_module.BookHashIndex.compute_content_md5(cls.alpha_path)
        cls.beta_filename_md5 = cls.sync_module.BookHashIndex.compute_filename_md5(cls.beta_path)
        cls.beta_content_md5 = cls.sync_module.BookHashIndex.compute_content_md5(cls.beta_path)

        cls.username = 'norm_user'
        cls.password_md5 = hashlib.md5(b'norm-secret').hexdigest()

        cls.httpd = ThreadedTCPServer(('localhost', 0), cls.server_mod.UnifiedHandler)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()
        time.sleep(0.1)

        status, _ = cls._post_json(
            cls.port,
            '/koreader/sync/users/create',
            {'username': cls.username, 'password': cls.password_md5},
        )
        assert status in (201, 409), f'Unexpected register status: {status}'

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.thread.join(timeout=1)
        cls.library_dir.cleanup()
        if os.path.exists(cls.db_file.name):
            os.unlink(cls.db_file.name)
        for key, value in cls._orig_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        importlib.reload(importlib.import_module('controllers.koreader_sync'))
        importlib.reload(importlib.import_module('controllers.opds'))
        importlib.reload(importlib.import_module('routes'))
        importlib.reload(importlib.import_module('server'))

    @staticmethod
    def _post_json(port, path, body, auth=None):
        conn = http.client.HTTPConnection('localhost', port, timeout=5)
        headers = {'Content-Type': 'application/json'}
        if auth:
            headers['Authorization'] = auth
        conn.request('POST', path, body=json.dumps(body), headers=headers)
        response = conn.getresponse()
        payload = response.read()
        conn.close()
        data = json.loads(payload.decode('utf-8')) if payload else None
        return response.status, data

    def _auth_headers(self):
        return {
            'X-Auth-User': self.username,
            'X-Auth-Key': self.password_md5,
            'Content-Type': 'application/json',
        }

    def _put(self, path, body):
        conn = http.client.HTTPConnection('localhost', self.port, timeout=5)
        conn.request('PUT', path, body=json.dumps(body), headers=self._auth_headers())
        response = conn.getresponse()
        payload = response.read()
        conn.close()
        data = json.loads(payload.decode('utf-8')) if payload else None
        return response.status, data

    def _get(self, path):
        conn = http.client.HTTPConnection('localhost', self.port, timeout=5)
        conn.request('GET', path, headers=self._auth_headers())
        response = conn.getresponse()
        payload = response.read()
        conn.close()
        data = json.loads(payload.decode('utf-8')) if payload else None
        return response.status, data

    def test_put_with_filename_hash_stores_under_canonical_id(self):
        status, data = self._put(
            '/koreader/sync/syncs/progress',
            {
                'document': self.alpha_filename_md5,
                'percentage': 25.5,
                'progress': 'page:7',
                'device': 'reader-A',
                'device_id': 'devA',
            },
        )
        self.assertEqual(status, 200, data)
        self.assertEqual(data['document'], self.alpha_content_md5)

    def test_get_with_content_hash_returns_record_stored_via_filename_hash(self):
        # Two different clients writing/reading the same epub via different
        # hash schemes must converge on the same record.
        put_status, put_data = self._put(
            '/koreader/sync/syncs/progress',
            {
                'document': self.beta_filename_md5,
                'percentage': 80,
                'progress': 'page:200',
                'device': 'reader-A',
                'device_id': 'devA',
            },
        )
        self.assertEqual(put_status, 200, put_data)

        # Read it back using the partial-content hash from the "other" client.
        get_status, get_data = self._get(
            f'/koreader/sync/syncs/progress/{self.beta_content_md5}'
        )
        self.assertEqual(get_status, 200, get_data)
        self.assertEqual(get_data['progress'], 'page:200')
        self.assertEqual(get_data['percentage'], 80)
        self.assertEqual(get_data['device'], 'reader-A')
        self.assertEqual(get_data['document'], self.beta_content_md5)

    def test_second_client_overwrites_position_for_same_book(self):
        # Client 1 writes via filename hash.
        self._put(
            '/koreader/sync/syncs/progress',
            {
                'document': self.alpha_filename_md5,
                'percentage': 10,
                'progress': 'page:1',
                'device': 'reader-A',
                'device_id': 'devA',
            },
        )
        # Client 2 writes via content hash for the same book.
        status, data = self._put(
            '/koreader/sync/syncs/progress',
            {
                'document': self.alpha_content_md5,
                'percentage': 50,
                'progress': 'page:42',
                'device': 'reader-B',
                'device_id': 'devB',
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(data['document'], self.alpha_content_md5)

        # GET with either hash returns client 2's position.
        for hash_value in (self.alpha_filename_md5, self.alpha_content_md5):
            get_status, get_data = self._get(f'/koreader/sync/syncs/progress/{hash_value}')
            self.assertEqual(get_status, 200)
            self.assertEqual(get_data['progress'], 'page:42')
            self.assertEqual(get_data['percentage'], 50)
            self.assertEqual(get_data['device'], 'reader-B')

    def test_put_with_unknown_hash_is_rejected(self):
        status, data = self._put(
            '/koreader/sync/syncs/progress',
            {
                'document': '0' * 32,
                'percentage': 10,
                'progress': 'page:1',
                'device': 'reader-A',
                'device_id': 'devA',
            },
        )
        self.assertEqual(status, 404)
        self.assertEqual(data['status'], 'error')
        self.assertEqual(data['code'], self.sync_module.KoReaderSyncController.ERROR_DOCUMENT_NOT_FOUND)

    def test_get_with_unknown_hash_is_rejected(self):
        status, data = self._get('/koreader/sync/syncs/progress/' + 'f' * 32)
        self.assertEqual(status, 404)
        self.assertEqual(data['status'], 'error')
        self.assertEqual(data['code'], self.sync_module.KoReaderSyncController.ERROR_DOCUMENT_NOT_FOUND)

    def test_get_with_known_hash_no_record_returns_empty(self):
        # Use a fresh book that has never been written to.
        fresh_path = os.path.join(self.library_dir.name, 'gamma.epub')
        create_epub(fresh_path, 'Gamma Title', 'Author Three', body_size=1)
        gamma_filename_md5 = self.sync_module.BookHashIndex.compute_filename_md5(fresh_path)

        status, data = self._get(f'/koreader/sync/syncs/progress/{gamma_filename_md5}')
        self.assertEqual(status, 200)
        self.assertEqual(data, {})

    def test_plain_filename_is_rejected_when_not_a_hash(self):
        # In normalize mode, clients must send a hash. A plain filename does
        # not match any cached hash, so it must be rejected.
        status, data = self._put(
            '/koreader/sync/syncs/progress',
            {
                'document': 'alpha.epub',
                'percentage': 10,
                'progress': 'page:1',
                'device': 'reader-A',
                'device_id': 'devA',
            },
        )
        self.assertEqual(status, 404)
        self.assertEqual(data['code'], self.sync_module.KoReaderSyncController.ERROR_DOCUMENT_NOT_FOUND)


if __name__ == '__main__':
    unittest.main()
