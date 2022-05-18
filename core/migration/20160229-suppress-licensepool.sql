ALTER TABLE licensepools ADD COLUMN suppressed boolean default false;
create index "ix_licensepools_suppressed" on licensepools (suppressed);
