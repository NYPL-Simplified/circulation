# encoding: utf-8
import random
import pytest
from psycopg2.extras import NumericRange
from sqlalchemy.exc import IntegrityError
from ...classifier import Classifier
from ...model import (
    create,
    get_one,
    get_one_or_create,
)
from ...model.classification import (
    Subject,
    Genre,
)


class TestSubject:

    def test_subject_lookup_errors(self, db_session):
        """
        GIVEN: A Subject
        WHEN:  Looking up the subject with missing parameters
        THEN:  The correct error is raised
        """
        with pytest.raises(ValueError) as excinfo:
            Subject.lookup(db_session, None, "identifier", "name")
        assert "Cannot look up Subject with no type." in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            Subject.lookup(db_session, Subject.TAG, None, None)
        assert "Cannot look up Subject when neither identifier nor name is provided." in str(excinfo.value)

    def test_subject_lookup_autocreate(self, db_session):
        """
        GIVEN: A Subject
        WHEN:  Looking up a subject that doesn't exist
        THEN:  The subject is created
        """

        # By default, Subject.lookup creates a Subject that doesn't exist.
        identifier = str(random.randint(1, 9999))
        name = str(random.randint(1, 9999))
        subject, is_new = Subject.lookup(
            db_session, Subject.TAG, identifier, name
        )
        assert True == is_new
        assert identifier == subject.identifier
        assert name == subject.name

        # But you can tell it not to autocreate.
        identifier2 = str(random.randint(1, 9999))
        subject, is_new = Subject.lookup(
            db_session, Subject.TAG, identifier2, None, autocreate=False
        )
        assert False == is_new
        assert None == subject

    def test_subject_lookup_by_name(self, db_session, create_subject):
        """
        GIVEN: A Subject
        WHEN:  Looking up a subject and there are two subjects with the same name
        THEN:  Subject.lookup treats the subject as interchangeable
        """

        # We can look up a subject by its name, without providing an identifier.
        subject1 = create_subject(db_session, Subject.TAG, "integration1")
        subject1.name = "A tag"
        assert (subject1, False)  == Subject.lookup(db_session, Subject.TAG, None, "A tag")

        # If we somehow get into a state where there are two Subjects
        # with the same name, Subject.lookup treats them as interchangeable.
        subject2 = create_subject(db_session, Subject.TAG, "integration2")
        subject2.name = "A tag"

        subject, is_new = Subject.lookup(db_session, Subject.TAG, None, "A tag")
        assert subject in [subject1, subject2]
        assert False == is_new

    def test_subject_assing_to_genre_can_remove_genre(self, db_session):
        """
        GIVEN: A Subject
        WHEN:  The genre and audience data for this subject is totally wrong
        THEN:  Calling assing_to_genre() will fix it
        """

        # Here's a Subject that identifies children's books.
        subject, _ = Subject.lookup(db_session, Subject.TAG, "Children's books", None)

        # The genre and audience data for this Subject is totally wrong.
        subject.audience = Classifier.AUDIENCE_ADULT
        subject.target_age = NumericRange(1, 10)
        subject.fiction = False
        science_fiction, _ = Genre.lookup(db_session, "Science Fiction")
        subject.genre = science_fiction

        # But calling assign_to_genre() will fix it
        subject.assign_to_genre()
        assert Classifier.AUDIENCE_CHILDREN == subject.audience
        assert NumericRange(None, None, '[]') == subject.target_age
        assert None == subject.genre
        assert None == subject.fiction


class TestGenre:
    def test_genre_full_table_cache(self, db_session, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        # We use Genre as a convenient way of testing
        # HasFullTableCache.populate_cache, which requires a real
        # SQLAlchemy ORM class to operate on.

        # We start with an unusable object as the cache
        assert Genre.RESET == Genre._cache
        assert Genre.RESET == Genre._id_cache

        # When we call populate_cache()...
        Genre.populate_cache(db_session)

        # Every Genre in the database is copied to the cache.
        dont_call_this = object
        drama, is_new = Genre.by_cache_key(db_session, "Drama", dont_call_this)
        assert "Drama" == drama.name
        assert False == is_new

        # The ID of every genre is copied to the ID cache.
        assert drama == Genre._id_cache[drama.id]
        drama2 = Genre.by_id(db_session, drama.id)
        assert drama2 == drama

    def test_genre_by_id(self, db_session, init_datasource_and_genres):
        """
        GIVEN: A Genre from get_one() db lookup
        WHEN: Looking up the genre by id
        THEN: The genre is located in the genre _id_cache
        """

        # Get a genre to test with.
        drama = get_one(db_session, Genre, name="Drama")

        # Since we went right to the database, that didn't change the
        # fact that the ID cache is uninitialized.
        assert Genre.RESET == Genre._id_cache

        # Look up the same genre using by_id...
        Genre.populate_cache(db_session)
        assert drama == Genre.by_id(db_session, drama.id)

        # ... and the ID cache is fully initialized.
        assert drama == Genre._id_cache[drama.id]
        assert len(Genre._id_cache) > 1

    def test_genre_by_cache_key_miss_triggers_create_function(self, db_session, init_datasource_and_genres):
        """
        GIVEN: A Genre lookup
        WHEN: There is a cache miss
        THEN: The genre is created
        """

        class Factory:
            def __init__(self):
                self.called = False

            def call_me(self):
                self.called = True
                genre, is_new = get_one_or_create(db_session, Genre, name="Drama")
                return genre, is_new

        factory = Factory()
        Genre._cache = {}
        Genre._id_cache = {}
        genre, is_new = Genre.by_cache_key(db_session, "Drama", factory.call_me)

        assert "Drama" == genre.name
        assert False == is_new
        assert True == factory.called

        # The Genre object created in call_me has been associated with the
        # Genre's cache key in the table-wide cache.
        assert genre == Genre._cache[genre.cache_key()]

        # The cache by ID has been similarly populated.
        assert genre == Genre._id_cache[genre.id]

    def test_genre_by_cache_key_miss_when_cache_is_reset_populates_cache(self, db_session, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """

        # The cache is not in a state to be used.
        assert Genre._cache == Genre.RESET

        # Call Genreby_cache_key...
        drama, is_new = Genre.by_cache_key(
            db_session, "Drama",
            lambda: get_one_or_create(db_session, Genre, name="Drama")
        )
        assert "Drama" == drama.name
        assert False == is_new

        # ... and the cache is repopulated
        assert drama.cache_key() in Genre._cache
        assert drama.id in Genre._id_cache

    def test_genre_by_cache_key_hit_returns_cached_object(self, db_session):
        """
        GIVEN:
        WHEN:
        THEN:
        """

        # If the object we ask for is not already in the cache, this
        # function will be called and raise an exception.
        def exploding_create_hook():
            raise Exception("Kaboom")
        drama, ignore = get_one_or_create(db_session, Genre, name="Drama")
        Genre._cache = { "Drama": drama }
        drama2, is_new = Genre.by_cache_key(
            db_session, "Drama", exploding_create_hook
        )

        # The object was already in the cache, so we just looked it up.
        # No exception.
        assert drama == drama2
        assert False == is_new

    def test_genre_name_is_unique(self, db_session):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        genre1, _ = Genre.lookup(db_session, "A Genre", autocreate=True)
        genre2, _ = Genre.lookup(db_session, "A Genre", autocreate=True)
        assert genre1 == genre2

        pytest.raises(IntegrityError, create, db_session, Genre, name="A Genre")

    def test_genre_default_fiction(self, db_session, init_datasource_and_genres):
        """
        GIVEN:
        WHEN:
        THEN:
        """
        science_fiction, _ = Genre.lookup(db_session, "Science Fiction")
        nonfiction, _ = Genre.lookup(db_session, "History")
        assert True == science_fiction.default_fiction
        assert False == nonfiction.default_fiction

        # Create a previously unknown genre.
        genre, _ = Genre.lookup(
            db_session, "Some Weird Genre", autocreate=True
        )

        # We don't know its default fiction status.
        assert None == genre.default_fiction
