-- Move the old table out of the way.

ALTER TABLE timestamps RENAME TO timestamps_old;

-- Create the new table.

CREATE TABLE timestamps (
    id bigserial NOT NULL,
    service character varying(255) NOT NULL,
    collection_id integer,
    "timestamp" timestamp without time zone,
    counter integer
);

-- Copy preexisting timestamps from the old table to the new, giving
-- each one an ascending value for the new primary key.

insert into timestamps (service, timestamp, counter) select t.service, t.timestamp, t.counter from timestamps_old as t order by t.service;

-- Drop the old table now that the new one exists.

drop table timestamps_old;
