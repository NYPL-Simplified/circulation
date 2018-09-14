# encoding: utf-8
from nose.tools import set_trace
from sqlalchemy import event

from . import directly_modified
from bibliographic_metadata import DataSource
from classification import Genre
from collection import Collection
from configuration import (
    Admin,
    AdminRole,
    ConfigurationSetting,
    ExternalIntegration,
)
from library import Library
from licensing import (
    DeliveryMechanism,
    LicensePool,
)
from session_manager import site_configuration_has_changed
from works import Work

# Most of the time, we can know whether a change to the database is
# likely to require that the application reload the portion of the
# configuration it gets from the database. These hooks will call
# site_configuration_has_changed() whenever such a change happens.
#
# This is not supposed to be a comprehensive list of changes that
# should trigger a ConfigurationSetting reload -- that needs to be
# handled on the application level -- but it should be good enough to
# catch most that slip through the cracks.
@event.listens_for(Collection.children, 'append')
@event.listens_for(Collection.children, 'remove')
@event.listens_for(Collection.libraries, 'append')
@event.listens_for(Collection.libraries, 'remove')
@event.listens_for(ExternalIntegration.settings, 'append')
@event.listens_for(ExternalIntegration.settings, 'remove')
@event.listens_for(Library.integrations, 'append')
@event.listens_for(Library.integrations, 'remove')
@event.listens_for(Library.settings, 'append')
@event.listens_for(Library.settings, 'remove')
def configuration_relevant_collection_change(target, value, initiator):
    site_configuration_has_changed(target)

@event.listens_for(Library, 'after_insert')
@event.listens_for(Library, 'after_delete')
@event.listens_for(ExternalIntegration, 'after_insert')
@event.listens_for(ExternalIntegration, 'after_delete')
@event.listens_for(Collection, 'after_insert')
@event.listens_for(Collection, 'after_delete')
@event.listens_for(ConfigurationSetting, 'after_insert')
@event.listens_for(ConfigurationSetting, 'after_delete')
def configuration_relevant_lifecycle_event(mapper, connection, target):
    site_configuration_has_changed(target)

@event.listens_for(Library, 'after_update')
@event.listens_for(ExternalIntegration, 'after_update')
@event.listens_for(Collection, 'after_update')
@event.listens_for(ConfigurationSetting, 'after_update')
def configuration_relevant_update(mapper, connection, target):
    if directly_modified(target):
        site_configuration_has_changed(target)

@event.listens_for(Admin, 'after_insert')
@event.listens_for(Admin, 'after_delete')
@event.listens_for(Admin, 'after_update')
def refresh_admin_cache(mapper, connection, target):
    # The next time someone tries to access an Admin,
    # the cache will be repopulated.
    Admin.reset_cache()

@event.listens_for(AdminRole, 'after_insert')
@event.listens_for(AdminRole, 'after_delete')
@event.listens_for(AdminRole, 'after_update')
def refresh_admin_role_cache(mapper, connection, target):
    # The next time someone tries to access an AdminRole,
    # the cache will be repopulated.
    AdminRole.reset_cache()

@event.listens_for(Collection, 'after_insert')
@event.listens_for(Collection, 'after_delete')
@event.listens_for(Collection, 'after_update')
def refresh_collection_cache(mapper, connection, target):
    # The next time someone tries to access a Collection,
    # the cache will be repopulated.
    Collection.reset_cache()

@event.listens_for(ConfigurationSetting, 'after_insert')
@event.listens_for(ConfigurationSetting, 'after_delete')
@event.listens_for(ConfigurationSetting, 'after_update')
def refresh_configuration_settings(mapper, connection, target):
    # The next time someone tries to access a configuration setting,
    # the cache will be repopulated.
    ConfigurationSetting.reset_cache()

@event.listens_for(DataSource, 'after_insert')
@event.listens_for(DataSource, 'after_delete')
@event.listens_for(DataSource, 'after_update')
def refresh_datasource_cache(mapper, connection, target):
    # The next time someone tries to access a DataSource,
    # the cache will be repopulated.
    DataSource.reset_cache()

@event.listens_for(DeliveryMechanism, 'after_insert')
@event.listens_for(DeliveryMechanism, 'after_delete')
@event.listens_for(DeliveryMechanism, 'after_update')
def refresh_datasource_cache(mapper, connection, target):
    # The next time someone tries to access a DeliveryMechanism,
    # the cache will be repopulated.
    DeliveryMechanism.reset_cache()

@event.listens_for(ExternalIntegration, 'after_insert')
@event.listens_for(ExternalIntegration, 'after_delete')
@event.listens_for(ExternalIntegration, 'after_update')
def refresh_datasource_cache(mapper, connection, target):
    # The next time someone tries to access an ExternalIntegration,
    # the cache will be repopulated.
    ExternalIntegration.reset_cache()

@event.listens_for(Genre, 'after_insert')
@event.listens_for(Genre, 'after_delete')
@event.listens_for(Genre, 'after_update')
def refresh_genre_cache(mapper, connection, target):
    # The next time someone tries to access a genre,
    # the cache will be repopulated.
    #
    # The only time this should really happen is the very first time a
    # site is brought up, but just in case.
    Genre.reset_cache()

@event.listens_for(Library, 'after_insert')
@event.listens_for(Library, 'after_delete')
@event.listens_for(Library, 'after_update')
def refresh_library_cache(mapper, connection, target):
    # The next time someone tries to access a library,
    # the cache will be repopulated.
    Library.reset_cache()

@event.listens_for(LicensePool.work_id, 'set')
@event.listens_for(Work.presentation_edition_id, 'set')
def add_work_to_customlists_for_collection(pool_or_work, value, oldvalue, initiator):
    if isinstance(pool_or_work, LicensePool):
        work = pool_or_work.work
        pools = [pool_or_work]
    else:
        work = pool_or_work
        pools = work.license_pools

    if (not oldvalue or oldvalue is NO_VALUE) and value and work and work.presentation_edition:
        for pool in pools:
            if not pool.collection:
                # This shouldn't happen, but don't crash if it does --
                # the correct behavior is that the work not be added to
                # any CustomLists.
                continue
            for list in pool.collection.customlists:
                # Since the work was just created, we can assume that
                # there's already a pending registration for updating the
                # work's internal index, and decide not to create a
                # second one.
                list.add_entry(work, featured=True, update_external_index=False)
