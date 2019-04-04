-- Add a set_to_delete column to keep track if a collection should be deleted.
alter table collections add COLUMN "set_to_delete" boolean default false;