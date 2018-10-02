-- Remove the media requirement set for precreated lanes back when we
-- thought audiobooks and ebooks would have separate lane structures.
update lanes set media=NULL where media='{Book}';
