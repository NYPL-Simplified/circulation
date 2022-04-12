import contextlib
import logging
import os, sys
from lxml import etree

from io import BytesIO
from zipfile import ZipFile

from .http import HTTP


class EpubAccessor(object):

    CONTAINER_FILE = "META-INF/container.xml"
    IDPF_NAMESPACE = "http://www.idpf.org/2007/opf"

    @classmethod
    @contextlib.contextmanager
    def open_epub(cls, url, content=None):
        """Cracks open an EPUB to expose its contents

        :param url: A url representing the EPUB, only used for errors and in
            the absence of the `content` parameter
        :param content: A string representing the compressed EPUB

        :return: A tuple containing a ZipFile of the EPUB and the path to its
            package
        """
        if not (url or content):
            raise ValueError("Cannot open epub without url or content")
        if url and not content:
            # Get the epub from the url if no content has been made available.
            content = HTTP.get_with_timeout(url).content
        content = BytesIO(content)

        with ZipFile(content) as zip_file:
            if not cls.CONTAINER_FILE in zip_file.namelist():
                raise ValueError("Invalid EPUB file, not modifying: %s" % url)

            with zip_file.open(cls.CONTAINER_FILE) as container_file:
                container = container_file.read()
                rootfiles_element = etree.fromstring(container).find("{urn:oasis:names:tc:opendocument:xmlns:container}rootfiles")

                if rootfiles_element is None:
                    raise ValueError("Invalid EPUB file, not modifying: %s" % url)

                rootfile_element = rootfiles_element.find("{urn:oasis:names:tc:opendocument:xmlns:container}rootfile")
                if rootfile_element is None:
                    raise ValueError("Invalid EPUB file, not modifying: %s" % url)

                package_document_path = rootfile_element.get('full-path')
            yield zip_file, package_document_path

    @classmethod
    def get_element_from_package(cls, zip_file, package_document_path, element_tag):
        """Pulls one or more elements from the package_document"""
        [element] = cls.get_elements_from_package(
            zip_file, package_document_path, [element_tag]
        )
        return element

    @classmethod
    def get_elements_from_package(cls, zip_file, package_document_path, element_tags):
        """Pulls one or more elements from the package_document"""
        if not isinstance(element_tags, list):
            element_tags = [element_tags]
        elements = list()
        with zip_file.open(package_document_path) as package_file:
            package = package_file.read()
            for element_tag in element_tags:
                element = etree.fromstring(package).find(
                    "{%s}%s" % (cls.IDPF_NAMESPACE, element_tag)
                )
                if element is None:
                    raise ValueError("Invalid EPUB file: '%s' could not be found" % element_tag)
                elements.append(element)
        return elements
