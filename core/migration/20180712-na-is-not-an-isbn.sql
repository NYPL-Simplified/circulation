delete from equivalents where output_id in (
 select id from identifiers where type='ISBN' and identifier='n/a'
);
delete from equivalents where input_id in (
 select id from identifiers where type='ISBN' and identifier='n/a'
);
delete from identifiers where type='ISBN' and identifier='n/a';
