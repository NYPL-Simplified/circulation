drop index if exists mv_works_genres_work_id_genre_id;
create index mv_works_genres_work_id_genre_id on mv_works_editions_workgenres_datasources_identifiers (works_id, genre_id); 

