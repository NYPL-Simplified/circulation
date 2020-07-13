# encoding: utf-8
import datetime

import botocore
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
)
from mock import MagicMock
from nose.tools import (
    assert_raises,
    eq_,
)
from parameterized import parameterized

from . import (
    DatabaseTest
)
from ..mirror import MirrorUploader
from ..model import (
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Representation,
    create,
)
from ..s3 import (
    S3Uploader,
    MockS3Client,
    MultipartS3Upload,
    S3AddressingStyle)


class S3UploaderTest(DatabaseTest):

    def _integration(self, **settings):
        """Create and configure a simple S3 integration."""
        integration = self._external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL,
            settings=settings
        )
        integration.username = 'username'
        integration.password = 'password'
        return integration

    def _add_settings_value(self, settings, key, value):
        """Adds a value to settings dictionary

        :param settings: Settings dictionary
        :type settings: Dict

        :param key: Key
        :type key: string

        :param value: Value
        :type value: Any

        :return: Updated settings dictionary
        :rtype: Dict
        """
        if value:
            if settings:
                settings[key] = value

            else:
                settings = {
                    key: value
                }

        return settings

    def _create_s3_uploader(
            self,
            client_class=None,
            uploader_class=None,
            region=None,
            addressing_style=None,
            **settings):
        """Creates a new instance of S3 uploader

        :param client_class: (Optional) Custom class to be used instead of boto3's client class
        :type client_class: Optional[Type]

        :param: uploader_class: (Optional) Custom class which will be used insted of S3Uploader
        :type uploader_class: Optional[Type]

        :param region: (Optional) S3 region
        :type region: Optional[string]

        :param addressing_style: (Optional) S3 addressing style
        :type addressing_style: Optional[string]

        :param settings: Kwargs used for initializing an external integration
        :type: Optional[Dict]

        :return: New intance of S3 uploader
        :rtype: S3Uploader
        """
        settings = self._add_settings_value(settings, S3Uploader.S3_REGION, region)
        settings = self._add_settings_value(settings, S3Uploader.S3_ADDRESSING_STYLE, addressing_style)
        integration = self._integration(**settings)
        uploader_class = uploader_class or S3Uploader

        return uploader_class(integration, client_class=client_class)


class TestS3Uploader(S3UploaderTest):
    def test_names(self):
        # The NAME associated with this class must be the same as its
        # key in the MirrorUploader implementation registry, and it's
        # better if it's the same as the name of the external
        # integration.
        eq_(S3Uploader.NAME, ExternalIntegration.S3)
        eq_(S3Uploader,
            MirrorUploader.IMPLEMENTATION_REGISTRY[ExternalIntegration.S3])

    def test_instantiation(self):
        integration = self._external_integration(
            ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL
        )
        integration.username = 'your-access-key'
        integration.password = 'your-secret-key'
        integration.setting(S3Uploader.URL_TEMPLATE_KEY).value = 'a transform'
        uploader = MirrorUploader.implementation(integration)
        eq_(True, isinstance(uploader, S3Uploader))

        # The URL_TEMPLATE_KEY setting becomes the .url_transform
        # attribute on the S3Uploader object.
        eq_('a transform', uploader.url_transform)

    @parameterized.expand([
        (
            'empty_credentials',
            None,
            None
        ),
        (
            'empty_string_credentials',
            '',
            ''
        ),
        (
            'non_empty_string_credentials',
            'username',
            'password'
        )
    ])
    def test_initialization(self, name, username, password):
        # Arrange
        settings = {'username': username, 'password': password}
        integration = self._external_integration(
            ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL, settings=settings
        )
        client_class = MagicMock()

        # Act
        S3Uploader(integration, client_class=client_class)

        # Assert
        eq_(client_class.call_count, 2)

        service_name = client_class.call_args_list[0].args[0]
        region_name = client_class.call_args_list[0].kwargs['region_name']
        aws_access_key_id = client_class.call_args_list[0].kwargs['aws_access_key_id']
        aws_secret_access_key = client_class.call_args_list[0].kwargs['aws_secret_access_key']
        config = client_class.call_args_list[0].kwargs['config']
        eq_(service_name, 's3')
        eq_(region_name, S3Uploader.S3_DEFAULT_REGION)
        eq_(aws_access_key_id, None)
        eq_(aws_secret_access_key, None)
        eq_(config.signature_version, botocore.UNSIGNED)
        eq_(config.s3['addressing_style'], S3Uploader.S3_DEFAULT_ADDRESSING_STYLE)

        service_name = client_class.call_args_list[1].args[0]
        region_name = client_class.call_args_list[1].kwargs['region_name']
        aws_access_key_id = client_class.call_args_list[1].kwargs['aws_access_key_id']
        aws_secret_access_key = client_class.call_args_list[1].kwargs['aws_secret_access_key']
        eq_(service_name, 's3')
        eq_(region_name, S3Uploader.S3_DEFAULT_REGION)
        eq_(aws_access_key_id, username if username != '' else None)
        eq_(aws_secret_access_key, password if password != '' else None)
        assert 'config' not in client_class.call_args_list[1].kwargs

    def test_custom_client_class(self):
        """You can specify a client class to use instead of boto3.client."""
        integration = self._integration()
        uploader = S3Uploader(integration, MockS3Client)
        assert isinstance(uploader.client, MockS3Client)

    def test_get_bucket(self):
        buckets = {
            S3Uploader.OA_CONTENT_BUCKET_KEY: 'banana',
            S3Uploader.BOOK_COVERS_BUCKET_KEY: 'bucket'
        }
        buckets_plus_irrelevant_setting = dict(buckets)
        buckets_plus_irrelevant_setting['not-a-bucket-at-all'] = "value"
        uploader = self._create_s3_uploader(**buckets_plus_irrelevant_setting)

        # This S3Uploader knows about the configured buckets.  It
        # wasn't informed of the irrelevant 'not-a-bucket-at-all'
        # setting.
        eq_(buckets, uploader.buckets)

        # get_bucket just does a lookup in .buckets
        uploader.buckets['foo'] = object()
        result = uploader.get_bucket('foo')
        eq_(uploader.buckets['foo'], result)

    @parameterized.expand([
        (
            's3_url_with_path_without_slash',
            'a-bucket',
            'a-path',
            'https://a-bucket.s3.amazonaws.com/a-path',
            None
        ),
        (
            's3_dummy_url_with_path_without_slash',
            'dummy',
            'dummy',
            'https://dummy.s3.amazonaws.com/dummy',
            None
        ),
        (
            's3_path_style_url_with_path_without_slash',
            'a-bucket',
            'a-path',
            'https://s3.amazonaws.com/a-bucket/a-path',
            None,
            S3AddressingStyle.PATH.value
        ),
        (
            's3_path_style_dummy_url_with_path_without_slash',
            'dummy',
            'dummy',
            'https://s3.amazonaws.com/dummy/dummy',
            None,
            S3AddressingStyle.PATH.value
        ),
        (
            's3_url_with_path_with_slash',
            'a-bucket',
            '/a-path',
            'https://a-bucket.s3.amazonaws.com/a-path',
            None,
        ),
        (
            's3_path_style_url_with_path_with_slash',
            'a-bucket',
            '/a-path',
            'https://s3.amazonaws.com/a-bucket/a-path',
            None,
            S3AddressingStyle.PATH.value
        ),
        (
            's3_url_with_custom_region_and_path_without_slash',
            'a-bucket',
            'a-path',
            'https://a-bucket.s3.us-east-2.amazonaws.com/a-path',
            'us-east-2',
        ),
        (
            's3_path_style_url_with_custom_region_and_path_without_slash',
            'a-bucket',
            'a-path',
            'https://s3.us-east-2.amazonaws.com/a-bucket/a-path',
            'us-east-2',
            S3AddressingStyle.PATH.value
        ),
        (
            's3_url_with_custom_region_and_path_with_slash',
            'a-bucket',
            '/a-path',
            'https://a-bucket.s3.us-east-3.amazonaws.com/a-path',
            'us-east-3'
        ),
        (
            's3_path_style_url_with_custom_region_and_path_with_slash',
            'a-bucket',
            '/a-path',
            'https://s3.us-east-3.amazonaws.com/a-bucket/a-path',
            'us-east-3',
            S3AddressingStyle.PATH.value
        ),
        (
            'custom_http_url_and_path_without_slash',
            'http://a-bucket.com/',
            'a-path',
            'http://a-bucket.com/a-path',
            None
        ),
        (
            'custom_http_url_and_path_with_slash',
            'http://a-bucket.com/',
            '/a-path',
            'http://a-bucket.com/a-path',
            None
        ),
        (
            'custom_http_url_and_path_without_slash',
            'https://a-bucket.com/',
            'a-path',
            'https://a-bucket.com/a-path',
            None
        ),
        (
            'custom_http_url_and_path_with_slash',
            'https://a-bucket.com/',
            '/a-path',
            'https://a-bucket.com/a-path',
            None
        )
    ])
    def test_url(self, name, bucket, path, expected_result, region=None, addressing_style=None):
        # Arrange
        uploader = self._create_s3_uploader(region=region, addressing_style=addressing_style)

        # Act
        result = uploader.url(bucket, path)

        # Assert
        eq_(result, expected_result)

    @parameterized.expand([
        (
            'implicit_s3_url_template',
            'bucket',
            'the key',
            'https://bucket.s3.amazonaws.com/the%20key'
        ),
        (
            'implicit_s3_url_template_with_custom_region',
            'bucket',
            'the key',
            'https://bucket.s3.us-east-2.amazonaws.com/the%20key',
            None,
            'us-east-2'
        ),
        (
            'explicit_s3_url_template',
            'bucket',
            'the key',
            'https://bucket.s3.amazonaws.com/the%20key',
            S3Uploader.URL_TEMPLATE_DEFAULT
        ),
        (
            'explicit_s3_url_template_with_custom_region',
            'bucket',
            'the key',
            'https://bucket.s3.us-east-2.amazonaws.com/the%20key',
            S3Uploader.URL_TEMPLATE_DEFAULT,
            'us-east-2'
        ),
        (
            'http_url_template',
            'bucket',
            'the këy',
            'http://bucket/the%20k%C3%ABy',
            S3Uploader.URL_TEMPLATE_HTTP
        ),
        (
            'https_url_template',
            'bucket',
            'the këy',
            'https://bucket/the%20k%C3%ABy',
            S3Uploader.URL_TEMPLATE_HTTPS
        )
    ])
    def test_final_mirror_url(self, name, bucket, key, expected_result, url_transform=None, region=None):
        # Arrange
        uploader = self._create_s3_uploader(region=region)

        if url_transform:
            uploader.url_transform = url_transform

        # Act
        result = uploader.final_mirror_url(bucket, key)

        # Assert
        if not url_transform:
            eq_(S3Uploader.URL_TEMPLATE_DEFAULT, uploader.url_transform)

        eq_(result, expected_result)

    def test_key_join(self):
        """Test the code used to build S3 keys from parts."""
        parts = ["Gutenberg", b"Gutenberg ID", 1234, "Die Flügelmaus+.epub"]
        eq_('Gutenberg/Gutenberg%20ID/1234/Die%20Fl%C3%BCgelmaus%2B.epub',
            S3Uploader.key_join(parts))

    @parameterized.expand([
        (
            'with_gutenberg_cover_generator_data_source',
            'test-book-covers-s3-bucket',
            DataSource.GUTENBERG_COVER_GENERATOR,
            'https://test-book-covers-s3-bucket.s3.amazonaws.com/Gutenberg%20Illustrated/'
        ),
        (
            'with_overdrive_data_source',
            'test-book-covers-s3-bucket',
            DataSource.OVERDRIVE,
            'https://test-book-covers-s3-bucket.s3.amazonaws.com/Overdrive/'
        ),
        (
            'with_overdrive_data_source_and_scaled_size',
            'test-book-covers-s3-bucket',
            DataSource.OVERDRIVE,
            'https://test-book-covers-s3-bucket.s3.amazonaws.com/scaled/300/Overdrive/',
            300
        ),
        (
            'with_gutenberg_cover_generator_data_source_and_custom_region',
            'test-book-covers-s3-bucket',
            DataSource.GUTENBERG_COVER_GENERATOR,
            'https://test-book-covers-s3-bucket.s3.us-east-3.amazonaws.com/Gutenberg%20Illustrated/',
            None,
            'us-east-3'
        ),
        (
            'with_overdrive_data_source_and_custom_region',
            'test-book-covers-s3-bucket',
            DataSource.OVERDRIVE,
            'https://test-book-covers-s3-bucket.s3.us-east-3.amazonaws.com/Overdrive/',
            None,
            'us-east-3'
        ),
        (
            'with_overdrive_data_source_and_scaled_size_and_custom_region',
            'test-book-covers-s3-bucket',
            DataSource.OVERDRIVE,
            'https://test-book-covers-s3-bucket.s3.us-east-3.amazonaws.com/scaled/300/Overdrive/',
            300,
            'us-east-3'
        )
    ])
    def test_cover_image_root(self, name, bucket, data_source_name, expected_result, scaled_size=None, region=None):
        # Arrange
        uploader = self._create_s3_uploader(region=region)
        data_source = DataSource.lookup(self._db, data_source_name)

        # Act
        result = uploader.cover_image_root(bucket, data_source, scaled_size=scaled_size)

        # Assert
        eq_(result, expected_result)

    @parameterized.expand([
        (
            'with_default_region',
            'test-open-access-s3-bucket',
            'https://test-open-access-s3-bucket.s3.amazonaws.com/'
        ),
        (
            'with_custom_region',
            'test-open-access-s3-bucket',
            'https://test-open-access-s3-bucket.s3.us-east-3.amazonaws.com/',
            'us-east-3'
        )
    ])
    def test_content_root(self, name, bucket, expected_result, region=None):
        # Arrange
        uploader = self._create_s3_uploader(region=region)

        # Act
        result = uploader.content_root(bucket)

        # Assert
        eq_(result, expected_result)

    @parameterized.expand([
        (
            's3_url',
            'test-marc-s3-bucket',
            'SHORT',
            'https://test-marc-s3-bucket.s3.amazonaws.com/SHORT/'
        ),
        (
            's3_url_with_custom_region',
            'test-marc-s3-bucket',
            'SHORT',
            'https://test-marc-s3-bucket.s3.us-east-2.amazonaws.com/SHORT/',
            'us-east-2'
        ),
        (
            'custom_http_url',
            'http://my-feed/',
            'SHORT',
            'http://my-feed/SHORT/'
        ),
        (
            'custom_https_url',
            'https://my-feed/',
            'SHORT',
            'https://my-feed/SHORT/'
        ),
    ])
    def test_marc_file_root(self, name, bucket, library_name, expected_result, region=None):
        # Arrange
        uploader = self._create_s3_uploader(region=region)
        library = self._library(short_name=library_name)

        # Act
        result = uploader.marc_file_root(bucket, library)

        # Assert
        eq_(result, expected_result)

    @parameterized.expand([
        (
            'with_identifier',
            {S3Uploader.OA_CONTENT_BUCKET_KEY: 'thebooks'},
            'ABOOK',
            'https://thebooks.s3.amazonaws.com/Gutenberg%20ID/ABOOK.epub'
        ),
        (
            'with_custom_extension',
            {S3Uploader.OA_CONTENT_BUCKET_KEY: 'thebooks'},
            'ABOOK',
            'https://thebooks.s3.amazonaws.com/Gutenberg%20ID/ABOOK.pdf',
            'pdf'
        ),
        (
            'with_custom_dotted_extension',
            {S3Uploader.OA_CONTENT_BUCKET_KEY: 'thebooks'},
            'ABOOK',
            'https://thebooks.s3.amazonaws.com/Gutenberg%20ID/ABOOK.pdf',
            '.pdf'
        ),
        (
            'with_custom_data_source',
            {S3Uploader.OA_CONTENT_BUCKET_KEY: 'thebooks'},
            'ABOOK',
            'https://thebooks.s3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK.epub',
            None,
            DataSource.UNGLUE_IT
        ),
        (
            'with_custom_title',
            {S3Uploader.OA_CONTENT_BUCKET_KEY: 'thebooks'},
            'ABOOK',
            'https://thebooks.s3.amazonaws.com/Gutenberg%20ID/ABOOK/On%20Books.epub',
            None,
            None,
            'On Books'
        ),
        (
            'with_custom_extension_and_title_and_data_source',
            {S3Uploader.OA_CONTENT_BUCKET_KEY: 'thebooks'},
            'ABOOK',
            'https://thebooks.s3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/On%20Books.pdf',
            '.pdf',
            DataSource.UNGLUE_IT,
            'On Books'
        ),
        (
            'with_custom_extension_and_title_and_data_source_and_region',
            {S3Uploader.OA_CONTENT_BUCKET_KEY: 'thebooks'},
            'ABOOK',
            'https://thebooks.s3.us-east-3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/On%20Books.pdf',
            '.pdf',
            DataSource.UNGLUE_IT,
            'On Books',
            'us-east-3'
        ),
        (
            'with_protected_access_and_custom_extension_and_title_and_data_source_and_region',
            {S3Uploader.PROTECTED_CONTENT_BUCKET_KEY: 'thebooks'},
            'ABOOK',
            'https://thebooks.s3.us-east-3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/On%20Books.pdf',
            '.pdf',
            DataSource.UNGLUE_IT,
            'On Books',
            'us-east-3',
            False,
        )
    ])
    def test_book_url(
            self,
            name,
            buckets,
            identifier,
            expected_result,
            extension=None,
            data_source_name=None,
            title=None,
            region=None,
            open_access=True):
        # Arrange
        identifier = self._identifier(foreign_id=identifier)
        uploader = self._create_s3_uploader(region=region, **buckets)

        parameters = {'identifier': identifier, 'open_access': open_access}

        if extension:
            parameters['extension'] = extension
        if title:
            parameters['title'] = title

        if data_source_name:
            data_source = DataSource.lookup(self._db, DataSource.UNGLUE_IT)
            parameters['data_source'] = data_source

        # Act
        result = uploader.book_url(**parameters)

        # Assert
        eq_(result, expected_result)

    @parameterized.expand([
        (
            'without_scaled_size',
            {S3Uploader.BOOK_COVERS_BUCKET_KEY: 'thecovers'},
            DataSource.UNGLUE_IT,
            'ABOOK',
            'filename',
            'https://thecovers.s3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/filename'
        ),
        (
            'without_scaled_size_and_with_custom_region',
            {S3Uploader.BOOK_COVERS_BUCKET_KEY: 'thecovers'},
            DataSource.UNGLUE_IT,
            'ABOOK',
            'filename',
            'https://thecovers.s3.us-east-3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/filename',
            None,
            'us-east-3'
        ),
        (
            'with_scaled_size',
            {S3Uploader.BOOK_COVERS_BUCKET_KEY: 'thecovers'},
            DataSource.UNGLUE_IT,
            'ABOOK',
            'filename',
            'https://thecovers.s3.amazonaws.com/scaled/601/unglue.it/Gutenberg%20ID/ABOOK/filename',
            601
        ),
        (
            'with_scaled_size_and_custom_region',
            {S3Uploader.BOOK_COVERS_BUCKET_KEY: 'thecovers'},
            DataSource.UNGLUE_IT,
            'ABOOK',
            'filename',
            'https://thecovers.s3.us-east-3.amazonaws.com/scaled/601/unglue.it/Gutenberg%20ID/ABOOK/filename',
            601,
            'us-east-3'
        )
    ])
    def test_cover_image_url(
            self,
            name,
            buckets,
            data_source_name,
            identifier,
            filename,
            expected_result,
            scaled_size=None,
            region=None):
        # identifier = self._identifier(foreign_id="ABOOK")
        # buckets = {S3Uploader.BOOK_COVERS_BUCKET_KEY : 'thecovers'}
        # uploader = self._uploader(**buckets)
        # m = uploader.cover_image_url
        #
        # unglueit = DataSource.lookup(self._db, DataSource.UNGLUE_IT)
        # identifier = self._identifier(foreign_id="ABOOK")
        # eq_(u'https://s3.amazonaws.com/thecovers/scaled/601/unglue.it/Gutenberg+ID/ABOOK/filename',
        #     m(unglueit, identifier, "filename", scaled_size=601))

        # Arrange
        data_source = DataSource.lookup(self._db, data_source_name)
        identifier = self._identifier(foreign_id=identifier)
        uploader = self._create_s3_uploader(region=region, **buckets)

        # Act
        result = uploader.cover_image_url(data_source, identifier, filename, scaled_size=scaled_size)

        # Assert
        eq_(result, expected_result)

    @parameterized.expand([
        (
            'with_s3_bucket_and_end_time',
            'marc',
            'SHORT',
            'Lane',
            datetime.datetime(2020, 1, 1, 0, 0, 0),
            'https://marc.s3.amazonaws.com/SHORT/2020-01-01%2000%3A00%3A00/Lane.mrc'
        ),
        (
            'with_s3_bucket_and_end_time_and_start_time',
            'marc',
            'SHORT',
            'Lane',
            datetime.datetime(2020, 1, 2, 0, 0, 0),
            'https://marc.s3.amazonaws.com/SHORT/2020-01-01%2000%3A00%3A00-2020-01-02%2000%3A00%3A00/Lane.mrc',
            datetime.datetime(2020, 1, 1, 0, 0, 0),
        ),
        (
            'with_s3_bucket_and_end_time_and_start_time_and_custom_region',
            'marc',
            'SHORT',
            'Lane',
            datetime.datetime(2020, 1, 2, 0, 0, 0),
            'https://marc.s3.us-east-2.amazonaws.com/SHORT/2020-01-01%2000%3A00%3A00-2020-01-02%2000%3A00%3A00/Lane.mrc',
            datetime.datetime(2020, 1, 1, 0, 0, 0),
            'us-east-2'
        ),
        (
            'with_http_bucket_and_end_time_and_start_time',
            'http://marc',
            'SHORT',
            'Lane',
            datetime.datetime(2020, 1, 2, 0, 0, 0),
            'http://marc/SHORT/2020-01-01%2000%3A00%3A00-2020-01-02%2000%3A00%3A00/Lane.mrc',
            datetime.datetime(2020, 1, 1, 0, 0, 0)
        ),
        (
            'with_https_bucket_and_end_time_and_start_time',
            'https://marc',
            'SHORT',
            'Lane',
            datetime.datetime(2020, 1, 2, 0, 0, 0),
            'https://marc/SHORT/2020-01-01%2000%3A00%3A00-2020-01-02%2000%3A00%3A00/Lane.mrc',
            datetime.datetime(2020, 1, 1, 0, 0, 0)
        )
    ])
    def test_marc_file_url(
            self,
            name,
            bucket,
            library_name,
            lane_name,
            end_time,
            expected_result,
            start_time=None,
            region=None):
        # Arrange
        library = self._library(short_name=library_name)
        lane = self._lane(display_name=lane_name)
        buckets = {S3Uploader.MARC_BUCKET_KEY: bucket}
        uploader = self._create_s3_uploader(region=region, **buckets)

        # Act
        result = uploader.marc_file_url(library, lane, end_time, start_time)

        # Assert
        eq_(result, expected_result)

    @parameterized.expand([
        (
            's3_path_style_request_without_region',
            'https://s3.amazonaws.com/bucket/directory/filename.jpg',
            ('bucket', 'directory/filename.jpg')
        ),
        (
            's3_path_style_request_with_region',
            'https://s3.us-east-2.amazonaws.com/bucket/directory/filename.jpg',
            ('bucket', 'directory/filename.jpg')
        ),
        (
            's3_virtual_hosted_style_request_with_global_endpoint',
            'https://bucket.s3.amazonaws.com/directory/filename.jpg',
            ('bucket', 'directory/filename.jpg')
        ),
        (
            's3_virtual_hosted_style_request_with_dashed_region',
            'https://bucket.s3-us-east-2.amazonaws.com/directory/filename.jpg',
            ('bucket', 'directory/filename.jpg')
        ),
        (
            's3_virtual_hosted_style_request_with_dotted_region',
            'https://bucket.s3.us-east-2.amazonaws.com/directory/filename.jpg',
            ('bucket', 'directory/filename.jpg')
        ),
        (
            'http_url',
            'http://book-covers.nypl.org/directory/filename.jpg',
            ('book-covers.nypl.org', 'directory/filename.jpg')
        ),
        (
            'https_url',
            'https://book-covers.nypl.org/directory/filename.jpg',
            ('book-covers.nypl.org', 'directory/filename.jpg')
        ),
        (
            'http_url_with_escaped_symbols',
            'http://book-covers.nypl.org/directory/filename+with+spaces%21.jpg',
            ('book-covers.nypl.org', 'directory/filename with spaces!.jpg')
        ),
        (
            'http_url_with_escaped_symbols_but_unquote_set_to_false',
            'http://book-covers.nypl.org/directory/filename+with+spaces%21.jpg',
            ('book-covers.nypl.org', 'directory/filename+with+spaces%21.jpg'),
            False
        ),
    ])
    def test_bucket_and_filename(self, name, url, expected_result, unquote=True):
        # Act
        result = S3Uploader.bucket_and_filename(url, unquote)

        # Assert
        eq_(result, expected_result)

    def test_mirror_one(self):
        edition, pool = self._edition(with_license_pool=True)
        original_cover_location = "http://example.com/a-cover.png"
        content = open(
            self.sample_cover_path("test-book-cover.png"), 'rb'
        ).read()
        cover, ignore = pool.add_link(
            Hyperlink.IMAGE, original_cover_location, edition.data_source,
            Representation.PNG_MEDIA_TYPE,
            content=content
        )
        cover_rep = cover.resource.representation
        eq_(None, cover_rep.mirrored_at)

        original_epub_location = "https://books.com/a-book.epub"
        epub, ignore = pool.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, original_epub_location,
            edition.data_source, Representation.EPUB_MEDIA_TYPE,
            content="i'm an epub"
        )
        epub_rep = epub.resource.representation
        eq_(None, epub_rep.mirrored_at)

        s3 = self._create_s3_uploader(client_class=MockS3Client)

        # Mock final_mirror_url so we can verify that it's called with
        # the right arguments
        def mock_final_mirror_url(bucket, key):
            return "final_mirror_url was called with bucket %s, key %s" % (
                bucket, key
            )

        s3.final_mirror_url = mock_final_mirror_url

        book_url = "http://books-go/here.epub"
        cover_url = "http://s3.amazonaws.com/covers-go/here.png"
        s3.mirror_one(cover.resource.representation, cover_url)
        s3.mirror_one(epub.resource.representation, book_url)
        [[data1, bucket1, key1, args1, ignore1],
         [data2, bucket2, key2, args2, ignore2], ] = s3.client.uploads

        # Both representations have had .mirror_url set and been
        # mirrored to those URLs.
        assert data1.startswith(b'\x89')
        eq_("covers-go", bucket1)
        eq_("here.png", key1)
        eq_(Representation.PNG_MEDIA_TYPE, args1['ContentType'])
        assert (datetime.datetime.utcnow() - cover_rep.mirrored_at).seconds < 10

        eq_(b"i'm an epub", data2)
        eq_("books-go", bucket2)
        eq_("here.epub", key2)
        eq_(Representation.EPUB_MEDIA_TYPE, args2['ContentType'])

        # In both cases, mirror_url was set to the result of final_mirror_url.
        eq_(
            u'final_mirror_url was called with bucket books-go, key here.epub',
            epub_rep.mirror_url
        )
        eq_(
            u'final_mirror_url was called with bucket covers-go, key here.png',
            cover_rep.mirror_url
        )

        # mirrored-at was set when the representation was 'mirrored'
        for rep in epub_rep, cover_rep:
            assert (datetime.datetime.utcnow() - rep.mirrored_at).seconds < 10

    def test_mirror_failure(self):
        edition, pool = self._edition(with_license_pool=True)
        original_epub_location = "https://books.com/a-book.epub"
        epub, ignore = pool.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, original_epub_location,
            edition.data_source, Representation.EPUB_MEDIA_TYPE,
            content="i'm an epub"
        )
        epub_rep = epub.resource.representation

        uploader = self._create_s3_uploader(MockS3Client)

        # A network failure is treated as a transient error.
        uploader.client.fail_with = BotoCoreError()
        uploader.mirror_one(epub_rep, self._url)
        eq_(None, epub_rep.mirrored_at)
        eq_(None, epub_rep.mirror_exception)

        # An S3 credential failure is treated as a transient error.
        response = dict(
            Error=dict(
                Code=401,
                Message="Bad credentials",
            )
        )
        uploader.client.fail_with = ClientError(response, "SomeOperation")
        uploader.mirror_one(epub_rep, self._url)
        eq_(None, epub_rep.mirrored_at)
        eq_(None, epub_rep.mirror_exception)

        # Because the file was not successfully uploaded,
        # final_mirror_url was never called and mirror_url is
        # was not set.
        eq_(None, epub_rep.mirror_url)

        # A bug in the code is not treated as a transient error --
        # the exception propagates through.
        uploader.client.fail_with = Exception("crash!")
        assert_raises(Exception, uploader.mirror_one, epub_rep, self._url)

    def test_svg_mirroring(self):
        edition, pool = self._edition(with_license_pool=True)
        original = self._url

        # Create an SVG cover for the book.
        svg = """<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
  "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">

<svg xmlns="http://www.w3.org/2000/svg" width="100" height="50">
    <ellipse cx="50" cy="25" rx="50" ry="25" style="fill:blue;"/>
</svg>"""
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE, original, edition.data_source,
            Representation.SVG_MEDIA_TYPE,
            content=svg)

        # 'Upload' it to S3.
        s3 = self._create_s3_uploader(MockS3Client)
        s3.mirror_one(hyperlink.resource.representation, self._url)
        [[data, bucket, key, args, ignore]] = s3.client.uploads

        eq_(Representation.SVG_MEDIA_TYPE, args['ContentType'])
        assert b'svg' in data
        assert b'PNG' not in data

    def test_multipart_upload(self):
        class MockMultipartS3Upload(MultipartS3Upload):
            completed = None
            aborted = None

            def __init__(self, uploader, representation, mirror_to):
                self.parts = []
                MockMultipartS3Upload.completed = False
                MockMultipartS3Upload.aborted = False

            def upload_part(self, content):
                self.parts.append(content)

            def complete(self):
                MockMultipartS3Upload.completed = True

            def abort(self):
                MockMultipartS3Upload.aborted = True

        rep, ignore = create(
            self._db, Representation, url="http://books.mrc",
            media_type=Representation.MARC_MEDIA_TYPE)

        s3 = self._create_s3_uploader(MockS3Client)

        # Successful upload
        with s3.multipart_upload(rep, rep.url, upload_class=MockMultipartS3Upload) as upload:
            eq_([], upload.parts)
            eq_(False, upload.completed)
            eq_(False, upload.aborted)

            upload.upload_part("Part 1")
            upload.upload_part("Part 2")

            eq_(["Part 1", "Part 2"], upload.parts)

        eq_(True, MockMultipartS3Upload.completed)
        eq_(False, MockMultipartS3Upload.aborted)
        eq_(None, rep.mirror_exception)

        class FailingMultipartS3Upload(MockMultipartS3Upload):
            def upload_part(self, content):
                raise Exception("Error!")

        # Failed during upload
        with s3.multipart_upload(rep, rep.url, upload_class=FailingMultipartS3Upload) as upload:
            upload.upload_part("Part 1")

        eq_(False, MockMultipartS3Upload.completed)
        eq_(True, MockMultipartS3Upload.aborted)
        eq_("Error!", rep.mirror_exception)

        class AnotherFailingMultipartS3Upload(MockMultipartS3Upload):
            def complete(self):
                raise Exception("Error!")

        rep.mirror_exception = None
        # Failed during completion
        with s3.multipart_upload(rep, rep.url, upload_class=AnotherFailingMultipartS3Upload) as upload:
            upload.upload_part("Part 1")

        eq_(False, MockMultipartS3Upload.completed)
        eq_(True, MockMultipartS3Upload.aborted)
        eq_("Error!", rep.mirror_exception)

    @parameterized.expand([
        ('default_expiration_parameter', None, int(S3Uploader.S3_DEFAULT_PRESIGNED_URL_EXPIRATION)),
        ('empty_expiration_parameter', {S3Uploader.S3_PRESIGNED_URL_EXPIRATION: 100}, 100)
    ])
    def test_sign_url(self, name, expiration_settings, expected_expiration):
        # Arrange
        region = 'us-east-1'
        bucket = 'bucket'
        filename = 'filename'
        url = 'https://{0}.s3.{1}.amazonaws.com/{2}'.format(bucket, region, filename)
        expected_url = url + '?AWSAccessKeyId=KEY&Expires=1&Signature=S'
        s3_uploader = self._create_s3_uploader(region=region, **expiration_settings if expiration_settings else {})
        s3_uploader.bucket_and_filename = MagicMock(return_value=(bucket, filename))
        s3_uploader.client.generate_presigned_url = MagicMock(return_value=expected_url)

        # Act
        result = s3_uploader.sign_url(url)

        # Assert
        eq_(result, expected_url)
        s3_uploader.bucket_and_filename.assert_called_once_with(url)
        s3_uploader.client.generate_presigned_url.assert_called_once_with(
            'get_object',
            ExpiresIn=expected_expiration,
            Params={
                'Bucket': bucket,
                'Key': filename
            })


class TestMultiPartS3Upload(S3UploaderTest):
    def _representation(self):
        rep, ignore = create(
            self._db, Representation, url="http://bucket/books.mrc",
            media_type=Representation.MARC_MEDIA_TYPE)
        return rep

    def test_init(self):
        uploader = self._create_s3_uploader(MockS3Client)
        rep = self._representation()
        upload = MultipartS3Upload(uploader, rep, rep.url)
        eq_(uploader, upload.uploader)
        eq_(rep, upload.representation)
        eq_("bucket", upload.bucket)
        eq_("books.mrc", upload.filename)
        eq_(1, upload.part_number)
        eq_([], upload.parts)
        eq_(1, upload.upload.get("UploadId"))

        uploader.client.fail_with = Exception("Error!")
        assert_raises(Exception, MultipartS3Upload, uploader, rep, rep.url)

    def test_upload_part(self):
        uploader = self._create_s3_uploader(MockS3Client)
        rep = self._representation()
        upload = MultipartS3Upload(uploader, rep, rep.url)
        upload.upload_part("Part 1")
        upload.upload_part("Part 2")
        eq_([{'Body': 'Part 1', 'UploadId': 1, 'PartNumber': 1, 'Bucket': 'bucket', 'Key': 'books.mrc'},
             {'Body': 'Part 2', 'UploadId': 1, 'PartNumber': 2, 'Bucket': 'bucket', 'Key': 'books.mrc'}],
            uploader.client.parts)
        eq_(3, upload.part_number)
        eq_([{'ETag': 'etag', 'PartNumber': 1}, {'ETag': 'etag', 'PartNumber': 2}],
            upload.parts)

        uploader.client.fail_with = Exception("Error!")
        assert_raises(Exception, upload.upload_part, "Part 3")

    def test_complete(self):
        uploader = self._create_s3_uploader(MockS3Client)
        rep = self._representation()
        upload = MultipartS3Upload(uploader, rep, rep.url)
        upload.upload_part("Part 1")
        upload.upload_part("Part 2")
        upload.complete()
        eq_([{'Bucket': 'bucket', 'Key': 'books.mrc', 'UploadId': 1, 'MultipartUpload': {
            'Parts': [{'ETag': 'etag', 'PartNumber': 1}, {'ETag': 'etag', 'PartNumber': 2}],
        }}], uploader.client.uploads)

    def test_abort(self):
        uploader = self._create_s3_uploader(MockS3Client)
        rep = self._representation()
        upload = MultipartS3Upload(uploader, rep, rep.url)
        upload.upload_part("Part 1")
        upload.upload_part("Part 2")
        upload.abort()
        eq_([], uploader.client.parts)
