-- Create a new enumerated type for types of timestamps.
DO $$
  BEGIN
    CREATE TYPE service_type AS ENUM (
        'monitor',
        'coverage_provider',
        'script'
    );
  EXCEPTION
    WHEN duplicate_object THEN RAISE NOTICE 'service_type already exists, not creating it.';
  END;
$$;


-- Add columns to the timestamp table, including service_type for the
-- enumerated type.

DO $$
  BEGIN
    ALTER TABLE timestamps ADD COLUMN service_type service_type;
  EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'service_type column already exists, not creating it.';
  END;
$$;

CREATE INDEX if not exists ix_timestamps_service_type ON timestamps USING btree (service_type);

DO $$
  BEGIN
    ALTER TABLE timestamps ADD COLUMN start TIMESTAMP WITHOUT TIME ZONE;
  EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column timestamps already exists, not creating it.';
  END;
$$;

DO $$
  BEGIN
    ALTER TABLE timestamps ADD COLUMN achievements CHARACTER VARYING;
  EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column achievements already exists, not creating it.';
  END;
$$;

DO $$
  BEGIN
    ALTER TABLE timestamps ADD COLUMN exception CHARACTER VARYING;
  EXCEPTION
    WHEN duplicate_column THEN RAISE NOTICE 'column exception already exists, not creating it.';
  END;
$$;

DO $$
  BEGIN
    ALTER TABLE timestamps RENAME COLUMN timestamp TO finish;
  EXCEPTION
    WHEN undefined_column THEN RAISE NOTICE 'column timestamp does not exist, assuming already renamed.';
  END;
$$;

-- Set service_type for all known monitors, coverage providers, and
-- scripts.

-- First let's take care of the easy cases.

-- All 'monitor' and 'sweep' services are monitors.
update timestamps set service_type='monitor' where (
       service ilike '%monitor%'
       or service ilike '%sweep%'
);

-- All 'coverage' services are coverage providers.
update timestamps set service_type='coverage_provider' where service ilike '%coverage%';

-- The database migration timestamps are managed by scripts.
update timestamps set service_type='script' where (
       service like 'Database Migration%'
);

-- The metadata wrangler reaper is a coverage provider; all other reapers
-- are monitors.
update timestamps set service_type='coverage_provider' where (
       service='Metadata Wrangler Reaper'
);
update timestamps set service_type='monitor' where (
       service ilike '%reaper%' and service_type is null
);

-- Now we get into specific cases where it's not clear from the service
-- name what is what.

-- All RBdigital and search index services are monitors.
update timestamps set service_type='monitor' where (
       service ilike 'search index update%' or
       service ilike 'rbdigital%'
);

update timestamps set service_type='monitor' where service in (
       'Metadata Wrangler Collection Updates',
       'Metadata Wrangler Auxiliary Metadata Delivery',
       'Work Randomness Updater',
       'Overdrive Collection Overview'
);

update timestamps set service_type='coverage_provider' where (
       service ilike '%metadata wrangler collection registrar%'
);

update timestamps set service_type='coverage_provider' where service in (
       'OCLC Classify Identifier Lookup'
);

-- Finally, apart from the database migration scripts, which are
-- covered above, every timestamp that uses the counter is a Monitor.
update timestamps set service_type='monitor' where (
       counter is not null and service_type is null
);

-- Fill in the 'start' values -- they'll be replaced with more accurate values
-- as the scripts run.
update timestamps set start=finish where start is null;
