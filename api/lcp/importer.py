class LCPImporter(object):
    """Class implementing LCP import workflow"""

    def __init__(self, lcp_encryptor, lcp_server):
        """Initializes a new instance of LCPImporter class

        :param lcp_encryptor: LCPEncryptor object
        :type lcp_encryptor: encrypt.LCPEncryptor

        :param lcp_server: LCPServer object
        :type lcp_server: server.LCPServer
        """
        self._lcp_encryptor = lcp_encryptor
        self._lcp_server = lcp_server

    def import_book(self, db, file_path, identifier):
        """Encrypts a book and sends a notification to the LCP server

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param file_path: File path to the book to be encrypted
        :type file_path: string

        :param identifier: Book's identifier
        :type identifier: string

        :return: Encryption result
        :rtype: LCPEncryptionResult
        """
        encrypted_content = self._lcp_encryptor.encrypt(db, file_path, identifier)
        self._lcp_server.add_content(db, encrypted_content)
