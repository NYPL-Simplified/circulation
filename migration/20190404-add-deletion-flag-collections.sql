-- Add a marked_for_deletion column to keep track if a collection should be deleted.
alter table collections add COLUMN "marked_for_deletion" boolean default false;