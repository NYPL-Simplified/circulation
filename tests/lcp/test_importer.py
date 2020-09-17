import sqlalchemy
from mock import MagicMock, create_autospec

from api.lcp.encrypt import LCPEncryptionResult, LCPEncryptor
from api.lcp.importer import LCPImporter
from api.lcp.server import LCPServer


class TestLCPImporter(object):
    def test_import_book(self):
        # Arrange
        file_path = '/opt/readium/raw_books/book.epub'
        identifier = '123456789'
        encrypted_content = LCPEncryptionResult(
            content_id='1',
            content_encryption_key='12345',
            protected_content_location='/opt/readium/files/encrypted',
            protected_content_disposition='encrypted_book',
            protected_content_type='application/epub+zip',
            protected_content_length=12345,
            protected_content_sha256='12345'
        )
        lcp_encryptor = create_autospec(spec=LCPEncryptor)
        lcp_encryptor.encrypt = MagicMock(return_value=encrypted_content)
        lcp_server = create_autospec(spec=LCPServer)
        lcp_server.add_content = MagicMock()
        importer = LCPImporter(lcp_encryptor, lcp_server)
        db = create_autospec(spec=sqlalchemy.orm.session.Session)

        # Act
        importer.import_book(db, file_path, identifier)

        # Assert
        lcp_encryptor.encrypt.assert_called_once_with(db, file_path, identifier)
        lcp_server.add_content.assert_called_once_with(db, encrypted_content)

