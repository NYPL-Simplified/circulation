-- Move the old table out of the way.

ALTER TABLE timestamps RENAME TO timestamps_old;

-- Create the new table.

CREATE TABLE timestamps (
    id integer NOT NULL,
    service character varying(255) NOT NULL,
    collection_id integer,
    "timestamp" timestamp without time zone,
    counter integer
);

CREATE SEQUENCE timestamps_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE timestamps_id_seq OWNED BY timestamps.id;

-- Copy preexisting timestamps from the old table to the new, giving
-- each one an ascending value for the new primary key.

insert into timestamps (id, service, timestamp, counter) select row_number() over (order by t.service), t.service, t.timestamp, t.counter from timestamps_old as t;

-- Drop the old table now that the new one exists.

drop table timestamps_old;
