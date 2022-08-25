import pytest
from unittest import TestCase

from parameterized import parameterized

from core.util.webpub_manifest_parser.core.ast import (
    ArrayOfContributorsProperty,
    ArrayOfSubjectsProperty,
    Contributor,
)
from core.util.webpub_manifest_parser.core.parsers import (
    AnyOfParser,
    ArrayParser,
    NumberParser,
    StringParser,
    TypeParser,
    URIParser,
    ValueParsingError,
    find_parser,
)


class NumberParserTest(TestCase):
    @parameterized.expand(
        [("correct_number", "123"), ("incorrect_number", "abc", ValueParsingError)]
    )
    def test(self, _, value, expected_error_class=None):
        validator = NumberParser()

        if expected_error_class:
            with pytest.raises(expected_error_class):
                validator.parse(value)
        else:
            validator.parse(value)


class URIParserTest(TestCase):
    @parameterized.expand(
        [
            ("correct_uri", "http://example.com"),
            ("incorrect_uri", "123", ValueParsingError),
        ]
    )
    def test(self, _, value, expected_error_class=None):
        validator = URIParser()

        if expected_error_class:
            with pytest.raises(expected_error_class):
                validator.parse(value)
        else:
            validator.parse(value)


class FunctionsTest(TestCase):
    @parameterized.expand(
        [
            (
                "contributor_property",
                ArrayOfContributorsProperty.PARSER,
                TypeParser,
                [
                    (
                        ArrayParser(
                            AnyOfParser([StringParser(), TypeParser(Contributor)])
                        ),
                        TypeParser(Contributor),
                    ),
                    (None, TypeParser(Contributor)),
                ],
            ),
            (
                "subject_property",
                ArrayOfSubjectsProperty.PARSER,
                TypeParser,
                [
                    (
                        ArrayParser(
                            AnyOfParser([StringParser(), TypeParser(Contributor)])
                        ),
                        TypeParser(Contributor),
                    ),
                    (None, TypeParser(Contributor)),
                ],
            ),
        ]
    )
    def test_find_parser(self, _, parent_parser, child_parser_type, expected_results):
        # Act
        actual_results = find_parser(parent_parser, child_parser_type)

        # Assert
        self.assertEqual(len(expected_results), len(actual_results))

        for expected_result, actual_result in zip(expected_results, actual_results):
            expected_parent_parser, expected_child_parser = expected_result
            actual_parent_parser, actual_child_parser = expected_result

            if expected_parent_parser is None:
                self.assertIsNone(actual_parent_parser)
            else:
                self.assertEqual(
                    expected_parent_parser.__class__, actual_parent_parser.__class__
                )

