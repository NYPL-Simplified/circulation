import functools
import logging
from contextlib import contextmanager
from urllib.parse import quote, urlsplit, unquote_plus

import boto3
import botocore
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
)
from enum import Enum
from flask_babel import lazy_gettext as _
from .mirror import MirrorUploader
from .model import ExternalIntegration
from .model.configuration import (
    ConfigurationOption,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationAttributeType
)

class MultipartS3Upload():
    def __init__(self, uploader, representation, mirror_to):
        self.uploader = uploader
        self.representation = representation
        self.bucket, self.filename = uploader.split_url(mirror_to)
        media_type = representation.external_media_type
        self.part_number = 1
        self.parts = []

        self.upload = uploader.client.create_multipart_upload(
            Bucket=self.bucket,
            Key=self.filename,
            ContentType=media_type,
        )

    def upload_part(self, content):
        logging.info("Uploading part %s of %s" % (self.part_number, self.filename))
        result = self.uploader.client.upload_part(
            Body=content,
            Bucket=self.bucket,
            Key=self.filename,
            PartNumber=self.part_number,
            UploadId=self.upload.get("UploadId"),
        )
        self.parts.append(dict(ETag=result.get("ETag"), PartNumber=self.part_number))
        self.part_number += 1

    def complete(self):
        if not self.parts:
            logging.info("Upload of %s was empty, not mirroring" % self.filename)
            self.abort()
        else:
            self.uploader.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.filename,
                UploadId=self.upload.get("UploadId"),
                MultipartUpload=dict(Parts=self.parts),
            )
            mirror_url = self.uploader.final_mirror_url(self.bucket, self.filename)
            self.representation.set_as_mirrored(mirror_url)
            logging.info("MIRRORED %s" % self.representation.mirror_url)

    def abort(self):
        logging.info("Aborting upload of %s" % self.filename)
        self.uploader.client.abort_multipart_upload(
            Bucket=self.bucket,
            Key=self.filename,
            UploadId=self.upload.get("UploadId"),
        )


def _get_available_regions():
    """Returns a list of available S3 regions

    :return: List of available S3 regions
    :rtype: List[string]
    """
    session = boto3.session.Session()

    return session.get_available_regions(service_name='s3')


def _get_available_region_options():
    """Returns a list of available options for S3Uploader's Region configuration setting

    :return: List of available options for S3Uploader's Region configuration setting
    :rtype: List[Dict]
    """
    available_regions = sorted(_get_available_regions())
    options = [ConfigurationOption(region, region) for region in available_regions]

    return options


class S3AddressingStyle(Enum):
    """Enumeration of different addressing styles supported by boto"""

    VIRTUAL = 'virtual'
    PATH = 'path'
    AUTO = 'auto'


class S3UploaderConfiguration(ConfigurationGrouping):
    S3_REGION = 's3_region'
    S3_DEFAULT_REGION = 'us-east-1'

    S3_ADDRESSING_STYLE = 's3_addressing_style'
    S3_DEFAULT_ADDRESSING_STYLE = S3AddressingStyle.VIRTUAL.value

    S3_PRESIGNED_URL_EXPIRATION = 's3_presigned_url_expiration'
    S3_DEFAULT_PRESIGNED_URL_EXPIRATION = 3600

    BOOK_COVERS_BUCKET_KEY = 'book_covers_bucket'
    OA_CONTENT_BUCKET_KEY = 'open_access_content_bucket'
    PROTECTED_CONTENT_BUCKET_KEY = 'protected_content_bucket'
    MARC_BUCKET_KEY = 'marc_bucket'

    URL_TEMPLATE_KEY = 'bucket_name_transform'
    URL_TEMPLATE_HTTP = 'http'
    URL_TEMPLATE_HTTPS = 'https'
    URL_TEMPLATE_DEFAULT = 'identity'

    URL_TEMPLATES_BY_TEMPLATE = {
        URL_TEMPLATE_HTTP: 'http://%(bucket)s/%(key)s',
        URL_TEMPLATE_HTTPS: 'https://%(bucket)s/%(key)s',
        URL_TEMPLATE_DEFAULT: 'https://%(bucket)s.s3.%(region)s/%(key)s'
    }

    access_key = ConfigurationMetadata(
        key=ExternalIntegration.USERNAME,
        label=_('Access Key'),
        description='',
        type=ConfigurationAttributeType.TEXT,
        required=False
    )

    secret_key = ConfigurationMetadata(
        key=ExternalIntegration.PASSWORD,
        label=_('Secret Key'),
        description=_(
            'If the <em>Access Key</em> and <em>Secret Key</em> are not given here credentials '
            'will be used as outlined in the '
            '<a href="https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html'
            '#configuring-credentials">Boto3 documenation</a>. '
            'If <em>Access Key</em> is given, <em>Secrent Key</em> must also be given.'
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False
    )

    book_covers_bucket = ConfigurationMetadata(
        key=BOOK_COVERS_BUCKET_KEY,
        label=_('Book Covers Bucket'),
        description=_(
            'All book cover images encountered will be mirrored to this S3 bucket. '
            'Large images will be scaled down, and the scaled-down copies will also be uploaded to this bucket. '
            '<p>The bucket must already exist&mdash;it will not be created automatically.</p>'
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False
    )

    open_access_content_bucket = ConfigurationMetadata(
        key=OA_CONTENT_BUCKET_KEY,
        label=_('Open Access Content Bucket'),
        description=_(
            'All open-access books encountered will be uploaded to this S3 bucket. '
            '<p>The bucket must already exist&mdash;it will not be created automatically.</p>'
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False
    )

    protected_access_content_bucket = ConfigurationMetadata(
        key=PROTECTED_CONTENT_BUCKET_KEY,
        label=_('Protected Access Content Bucket'),
        description=_(
            'Self-hosted books will be uploaded to this S3 bucket. '
            '<p>The bucket must already exist&mdash;it will not be created automatically.</p>'
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False
    )

    marc_file_bucket = ConfigurationMetadata(
        key=MARC_BUCKET_KEY,
        label=_('MARC File Bucket'),
        description=_(
            'All generated MARC files will be uploaded to this S3 bucket. '
            '<p>The bucket must already exist&mdash;it will not be created automatically.</p>'
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False
    )

    s3_region = ConfigurationMetadata(
        key=S3_REGION,
        label=_('S3 region'),
        description=_(
            'S3 region which will be used for storing the content.'
        ),
        type=ConfigurationAttributeType.SELECT,
        required=False,
        default=S3_DEFAULT_REGION,
        options=_get_available_region_options()
    )

    s3_addressing_style = ConfigurationMetadata(
        key=S3_ADDRESSING_STYLE,
        label=_('S3 addressing style'),
        description=_(
            'Buckets created after September 30, 2020, will support only virtual hosted-style requests. '
            'Path-style requests will continue to be supported for buckets created on or before this date. '
            'For more information, '
            'see <a href="https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/">'
            'Amazon S3 Path Deprecation Plan - The Rest of the Story</a>.'
        ),
        type=ConfigurationAttributeType.SELECT,
        required=False,
        default=S3_DEFAULT_REGION,
        options=[
            ConfigurationOption(S3AddressingStyle.VIRTUAL.value, _('Virtual')),
            ConfigurationOption(S3AddressingStyle.PATH.value, _('Path')),
            ConfigurationOption(S3AddressingStyle.AUTO.value, _('Auto'))
        ]
    )

    s3_presigned_url_expiration = ConfigurationMetadata(
        key=S3_PRESIGNED_URL_EXPIRATION,
        label=_('S3 presigned URL expiration'),
        description=_(
            'Time in seconds for the presigned URL to remain valid'
        ),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=S3_DEFAULT_PRESIGNED_URL_EXPIRATION,
    )

    url_template = ConfigurationMetadata(
        key=URL_TEMPLATE_KEY,
        label=_('URL format'),
        description=_(
            'A file mirrored to S3 is available at <code>http://{bucket}.s3.{region}.amazonaws.com/{filename}</code>. '
            'If you\'ve set up your DNS so that http://[bucket]/ or https://[bucket]/ points to the appropriate '
            'S3 bucket, you can configure this S3 integration to shorten the URLs. '
            '<p>If you haven\'t set up your S3 buckets, don\'t change this from the default -- '
            'you\'ll get URLs that don\'t work.</p>'
        ),
        type=ConfigurationAttributeType.SELECT,
        required=False,
        default=URL_TEMPLATE_DEFAULT,
        options=[
            ConfigurationOption(
                URL_TEMPLATE_DEFAULT, _('S3 Default: https://{bucket}.s3.{region}.amazonaws.com/{file}')),
            ConfigurationOption(
                URL_TEMPLATE_HTTPS, _('HTTPS: https://{bucket}/{file}')),
            ConfigurationOption(
                URL_TEMPLATE_HTTP, _('HTTP: http://{bucket}/{file}'))
        ]
    )


class S3Uploader(MirrorUploader):
    NAME = ExternalIntegration.S3

    # AWS S3 host
    S3_HOST = 'amazonaws.com'

    SETTINGS = S3UploaderConfiguration.to_settings()

    SITEWIDE = True

    def __init__(self, integration, client_class=None, host=S3_HOST):
        """Instantiate an S3Uploader from an ExternalIntegration.

        :param integration: An ExternalIntegration
        :type integration: ExternalIntegration

        :param client_class: Mock object (or class) to use (or instantiate)
            instead of boto3.client.
        :type client_class: Any

        :param host: Host used by this integration
        :type host: string
        """
        super(S3Uploader, self).__init__(integration, host)

        if not client_class:
            client_class = boto3.client

        self._s3_region = integration.setting(
            S3UploaderConfiguration.S3_REGION).value_or_default(
            S3UploaderConfiguration.S3_DEFAULT_REGION)

        self._s3_addressing_style = integration.setting(
            S3UploaderConfiguration.S3_ADDRESSING_STYLE).value_or_default(
            S3UploaderConfiguration.S3_DEFAULT_ADDRESSING_STYLE)

        self._s3_presigned_url_expiration = integration.setting(
            S3UploaderConfiguration.S3_PRESIGNED_URL_EXPIRATION).value_or_default(
            S3UploaderConfiguration.S3_DEFAULT_PRESIGNED_URL_EXPIRATION)

        if callable(client_class):
            # Pass None into boto3 if we get an empty string.
            access_key = integration.username if integration.username != '' else None
            secret_key = integration.password if integration.password != '' else None
            config = Config(
                signature_version=botocore.UNSIGNED,
                s3={'addressing_style': self._s3_addressing_style}
            )
            # NOTE: Unfortunately, boto ignores credentials (aws_access_key_id, aws_secret_access_key)
            # when using botocore.UNSIGNED signature version and doesn't authenticate the client in this case.
            # That's why we have to create two S3 boto clients:
            # - the first client WITHOUT authentication which is used for generating unsigned URLs
            # - the second client WITH authentication used for working with S3: uploading files, etc.
            self._s3_link_client = client_class(
                's3',
                region_name=self._s3_region,
                aws_access_key_id=None,
                aws_secret_access_key=None,
                config=config
            )
            self.client = client_class(
                's3',
                region_name=self._s3_region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
        else:
            self.client = client_class

        self.url_transform = integration.setting(
            S3UploaderConfiguration.URL_TEMPLATE_KEY).value_or_default(
            S3UploaderConfiguration.URL_TEMPLATE_DEFAULT)

        # Transfer information about bucket names from the
        # ExternalIntegration to the S3Uploader object, so we don't
        # have to keep the ExternalIntegration around.
        self.buckets = dict()
        for setting in integration.settings:
            if setting.key.endswith('_bucket'):
                self.buckets[setting.key] = setting.value

    def _generate_s3_url(self, bucket, path):
        """Generates an S3 URL

        :param bucket: Bucket name
        :type bucket: string

        :return: S3 URL
        :rtype: string
        """
        key = path

        # NOTE: path can be an empty string meaning that
        # we need to generate a URL pointing at the root directory of the bucket.
        # However, boto3 doesn't allow us to pass the key as an empty string.
        # As a workaround we set it to a dummy string and later remove it from the generated URL
        if not path:
            key = 'dummy'

        url = self._s3_link_client.generate_presigned_url(
            'get_object',
            ExpiresIn=0,
            Params={
                'Bucket': bucket,
                'Key': key
            }
        )

        # If the path was an empty string we need to strip out trailing dummy string ending up with a URL
        # pointing at the root directory of the bucket
        if not path:
            url = url.replace('/' + key, '/')

        return url

    def sign_url(self, url, expiration=None):
        """Signs a URL and make it expirable

        :param url: URL
        :type url: string

        :param expiration: (Optional) Time in seconds for the presigned URL to remain valid.
            If it's empty, S3_PRESIGNED_URL_EXPIRATION configuration setting is used
        :type expiration: int

        :return: Signed expirable link
        :rtype: string
        """
        if not expiration:
            expiration = self._s3_presigned_url_expiration

        bucket, key = self.split_url(url)
        url = self.client.generate_presigned_url(
            'get_object',
            ExpiresIn=int(expiration),
            Params={
                'Bucket': bucket,
                'Key': key
            }
        )

        return url

    def get_bucket(self, bucket_key):
        """Gets the bucket for a particular use based on the given key"""
        return self.buckets.get(bucket_key)

    def url(self, bucket, path):
        """The URL to a resource on S3 identified by bucket and path."""
        custom_url = bucket.startswith('http://') or bucket.startswith('https://')

        if isinstance(path, list):
            # This is a list of key components that need to be quoted
            # and assembled.
            path = self.key_join(path, encode=custom_url)
        if isinstance(path, bytes):
            path = path.decode("utf-8")
        if path.startswith('/'):
            path = path[1:]

        if custom_url:
            url = bucket

            if not url.endswith('/'):
                url += '/'

            return url + path
        else:
            url = self._generate_s3_url(bucket, path)

            return url

    def cover_image_root(self, bucket, data_source, scaled_size=None):
        """The root URL to the S3 location of cover images for
        the given data source.
        """
        parts = []
        if scaled_size:
            parts.extend(["scaled", str(scaled_size)])
        if isinstance(data_source, str):
            data_source_name = data_source
        else:
            data_source_name = data_source.name
        parts.append(data_source_name)
        url = self.url(bucket, parts)
        if not url.endswith('/'):
            url += '/'
        return url

    def content_root(self, bucket):
        """The root URL to the S3 location of hosted content of
        the given type.
        """
        return self.url(bucket, '/')

    def marc_file_root(self, bucket, library):
        url = self.url(bucket, [library.short_name])
        if not url.endswith("/"):
            url += "/"
        return url

    @classmethod
    def key_join(self, key, encode=True):
        """Quote the path portions of an S3 key while leaving the path
        characters themselves alone.

        :param key: Either a key, or a list of parts to be
                    assembled into a key.

        :return: A string that can be used as an S3 key.
        """
        if isinstance(key, str):
            parts = key.split('/')
        else:
            parts = key
        new_parts = []

        for part in parts:
            if isinstance(part, bytes):
                part = part.decode("utf-8")
            if encode:
                part = quote(str(part))
            new_parts.append(part)

        return '/'.join(new_parts)

    def book_url(self, identifier, extension='.epub', open_access=True,
                 data_source=None, title=None):
        """The path to the hosted EPUB file for the given identifier."""
        bucket = self.get_bucket(
            S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY if open_access
            else S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY)
        root = self.content_root(bucket)

        if not extension.startswith('.'):
            extension = '.' + extension

        parts = []
        if data_source:
            parts.append(data_source.name)
        parts.append(identifier.type)
        if title:
            # e.g. DataSource/ISBN/1234/Title.epub
            parts.append(identifier.identifier)
            filename = title
        else:
            # e.g. DataSource/ISBN/1234.epub
            filename = identifier.identifier
        parts.append(filename + extension)
        return root + self.key_join(parts)

    def cover_image_url(self, data_source, identifier, filename,
                        scaled_size=None):
        """The path to the hosted cover image for the given identifier."""
        bucket = self.get_bucket(S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY)
        root = self.cover_image_root(bucket, data_source, scaled_size)
        parts = [identifier.type, identifier.identifier, filename]
        return root + self.key_join(parts)

    def marc_file_url(self, library, lane, end_time, start_time=None):
        """The path to the hosted MARC file for the given library, lane,
        and date range."""
        bucket = self.get_bucket(S3UploaderConfiguration.MARC_BUCKET_KEY)
        root = self.marc_file_root(bucket, library)
        if start_time:
            time_part = str(start_time) + "-" + str(end_time)
        else:
            time_part = str(end_time)
        parts = [time_part, lane.display_name]
        return root + self.key_join(parts) + ".mrc"

    def split_url(self, url, unquote=True):
        """Splits the URL into the components: bucket and file path

        :param url: URL
        :type url: string

        :param unquote: Boolean value indicating whether it's required to unquote URL elements
        :type unquote: bool

        :return: Tuple (bucket, file path)
        :rtype: Tuple[string, string]
        """
        scheme, netloc, path, query, fragment = urlsplit(url)

        if self.is_self_url(url):
            host_parts = netloc.split('.')
            host_parts_count = len(host_parts)

            # 1. Path-style requests
            # 1.1. URL without a region: https://s3.amazonaws.com/{bucket}/{path}
            # 1.2. URL with a region: https://s3.{region}.amazonaws.com/{bucket}/{path}

            # 2. Virtual hosted-style requests
            # 2.1. Legacy global endpoints: https://{bucket}.s3.amazonaws.com/{path}
            # 2.2. Endpoints with s3-region: https://{bucket}.s3-{region}.amazonaws.com/{path}
            # 2.3. Endpoints with s3.region: https://{bucket}.s3.{region}.amazonaws.com/{path}

            if host_parts_count == 3 or \
                    (host_parts_count == 4 and host_parts[0] == 's3'):
                if path.startswith('/'):
                    path = path[1:]
                bucket, filename = path.split('/', 1)
            else:
                bucket = host_parts[0]

                if path.startswith('/'):
                    path = path[1:]

                filename = path
        else:
            bucket = netloc
            filename = path[1:]

        if unquote:
            filename = unquote_plus(filename)

        return bucket, filename

    def final_mirror_url(self, bucket, key):
        """Determine the URL to pass into Representation.set_as_mirrored,
        assuming that it was successfully uploaded to the given
        `bucket` as `key`.

        Depending on ExternalIntegration configuration this may
        be any of the following:

        https://{bucket}.s3.{region}.amazonaws.com/{key}
        http://{bucket}/{key}
        https://{bucket}/{key}
        """
        templates = S3UploaderConfiguration.URL_TEMPLATES_BY_TEMPLATE
        default = templates[S3UploaderConfiguration.URL_TEMPLATE_DEFAULT]
        template = templates.get(self.url_transform, default)

        if template == default:
            link = self._generate_s3_url(bucket, self.key_join(key, encode=False))
        else:
            link = template % dict(bucket=bucket, key=self.key_join(key))

        return link

    def mirror_one(self, representation, mirror_to, collection=None):
        """Mirror a single representation to the given URL.

        :param representation: Book's representation
        :type representation: Representation

        :param mirror_to: Mirror URL
        :type mirror_to: string

        :param collection: Collection
        :type collection: Optional[core.model.collection.Collection]
        """
        # Turn the original URL into an s3.amazonaws.com URL.
        media_type = representation.external_media_type
        bucket, remote_filename = self.split_url(mirror_to)
        fh = representation.external_content()
        try:
            result = self.client.upload_fileobj(
                Fileobj=fh,
                Bucket=bucket,
                Key=remote_filename,
                ExtraArgs=dict(ContentType=media_type)
            )

            # Since upload_fileobj completed without a problem, we
            # know the file is available at
            # https://s3.amazonaws.com/{bucket}/{remote_filename}. But
            # that may not be the URL we want to store.
            mirror_url = self.final_mirror_url(bucket, remote_filename)
            representation.set_as_mirrored(mirror_url)

            source = representation.local_content_path
            if representation.url != mirror_url:
                source = representation.url
            if source:
                logging.info("MIRRORED %s => %s",
                             source, representation.mirror_url)
            else:
                logging.info("MIRRORED %s", representation.mirror_url)
        except (BotoCoreError, ClientError) as e:
            # BotoCoreError happens when there's a problem with
            # the network transport. ClientError happens when
            # there's a problem with the credentials. Either way,
            # the best thing to do is treat this as a transient
            # error and try again later. There's no scenario where
            # giving up is the right move.
            logging.error(
                "Error uploading %s: %r", mirror_to, e, exc_info=e
            )
        finally:
            fh.close()

    @contextmanager
    def multipart_upload(self, representation, mirror_to, upload_class=MultipartS3Upload):
        upload = upload_class(self, representation, mirror_to)
        try:
            yield upload
            upload.complete()
        except Exception as e:
            logging.error("Multipart upload of %s failed: %r", mirror_to, e, exc_info=e)
            upload.abort()
            representation.mirror_exception = str(e)


# MirrorUploader.implementation will instantiate an S3Uploader
# for storage integrations with protocol 'Amazon S3'.
MirrorUploader.IMPLEMENTATION_REGISTRY[S3Uploader.NAME] = S3Uploader


class MinIOUploaderConfiguration(ConfigurationGrouping):
    ENDPOINT_URL = 'ENDPOINT_URL'

    endpoint_url = ConfigurationMetadata(
        key=ENDPOINT_URL,
        label=_('Endpoint URL'),
        description=_(
            'MinIO\'s endpoint URL'
        ),
        type=ConfigurationAttributeType.TEXT,
        required=True
    )


class MinIOUploader(S3Uploader):
    NAME = ExternalIntegration.MINIO

    SETTINGS = S3Uploader.SETTINGS + [MinIOUploaderConfiguration.endpoint_url.to_settings()]

    def __init__(self, integration, client_class=None):
        """Instantiate an S3Uploader from an ExternalIntegration.

        :param integration: An ExternalIntegration

        :param client_class: Mock object (or class) to use (or instantiate)
            instead of boto3.client.
        """
        endpoint_url = integration.setting(
            MinIOUploaderConfiguration.ENDPOINT_URL).value

        _, host, _, _, _ = urlsplit(endpoint_url)

        if not client_class:
            client_class = boto3.client

        if callable(client_class):
            client_class = functools.partial(client_class, endpoint_url=endpoint_url)
        else:
            self.client = client_class

        super(MinIOUploader, self).__init__(integration, client_class, host)


# MirrorUploader.implementation will instantiate an MinIOUploader instance
# for storage integrations with protocol 'MinIO'.
MirrorUploader.IMPLEMENTATION_REGISTRY[MinIOUploader.NAME] = MinIOUploader


class MockS3Uploader(S3Uploader):
    """A dummy uploader for use in tests."""

    buckets = {
        S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: 'test-cover-bucket',
        S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: 'test-content-bucket',
        S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: 'test-content-bucket',
        S3UploaderConfiguration.MARC_BUCKET_KEY: 'test-marc-bucket',
    }

    def __init__(self, fail=False, *args, **kwargs):
        self.uploaded = []
        self.content = []
        self.destinations = []
        self.fail = fail
        self._s3_region = S3UploaderConfiguration.S3_DEFAULT_REGION
        self._s3_addressing_style = S3UploaderConfiguration.S3_DEFAULT_ADDRESSING_STYLE
        config = Config(
            signature_version=botocore.UNSIGNED,
            s3={'addressing_style': self._s3_addressing_style}
        )
        self._s3_link_client = boto3.client(
            's3',
            region_name=self._s3_region,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            config=config
        )
        self.client = boto3.client(
            's3',
            region_name=self._s3_region,
            aws_access_key_id=None,
            aws_secret_access_key=None,
        )

    def mirror_one(self, representation, **kwargs):
        mirror_to = kwargs['mirror_to']
        self.uploaded.append(representation)
        self.destinations.append(mirror_to)
        self.content.append(representation.content)
        if self.fail:
            representation.mirror_exception = "Exception"
            representation.mirrored_at = None
        else:
            representation.set_as_mirrored(mirror_to)

    @contextmanager
    def multipart_upload(self, representation, mirror_to):
        class MockMultipartS3Upload(MultipartS3Upload):
            def __init__(self):
                self.parts = []

            def upload_part(self, part):
                self.parts.append(part)

        upload = MockMultipartS3Upload()
        yield upload

        self.uploaded.append(representation)
        self.destinations.append(mirror_to)
        self.content.append(upload.parts)
        if self.fail:
            representation.mirror_exception = "Exception"
            representation.mirrored_at = None
        else:
            representation.set_as_mirrored(mirror_to)


class MockS3Client(object):
    """This pool lets us test the real S3Uploader class with a mocked-up
    boto3 client.
    """

    def __init__(self, service, region_name, aws_access_key_id, aws_secret_access_key, config=None):
        assert service == 's3'
        self.region_name = region_name
        self.access_key = aws_access_key_id
        self.secret_key = aws_secret_access_key
        self.config = config
        self.uploads = []
        self.parts = []
        self.fail_with = None

    def upload_fileobj(self, Fileobj, Bucket, Key, ExtraArgs=None, **kwargs):
        if self.fail_with:
            raise self.fail_with
        self.uploads.append((Fileobj.read(), Bucket, Key, ExtraArgs, kwargs))
        return None

    def create_multipart_upload(self, **kwargs):
        if self.fail_with:
            raise self.fail_with
        return dict(UploadId=1)

    def upload_part(self, **kwargs):
        if self.fail_with:
            raise self.fail_with
        self.parts.append(kwargs)
        return dict(ETag="etag")

    def complete_multipart_upload(self, **kwargs):
        self.uploads.append(kwargs)
        self.parts = []
        return None

    def abort_multipart_upload(self, **kwargs):
        self.parts = []
        return None

    def generate_presigned_url(
            self,
            ClientMethod,
            Params=None,
            ExpiresIn=3600,
            HttpMethod=None):
        return None
