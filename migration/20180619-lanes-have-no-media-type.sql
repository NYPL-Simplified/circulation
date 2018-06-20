-- The lanes.media column is no longer set by default -- the same lanes are
-- present for both ebooks and audiobooks, and an EntryPoint is used to
-- filter them.
--
-- We're not removing lanes.media altogether because it might be useful
-- as a way of dividing up sublanes for other entry points.
update lanes set media=null;
