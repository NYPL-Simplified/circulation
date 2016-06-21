update datasources set primary_identifier_type=NULL where name in (
       'Library Simplified Open Access Content Server', 
       'OCLC Classify',
       'OCLC Linked Data',
       'Amazon',
       'Library Simplified metadata wrangler',
       'Library Staff'
);
update datasources set primary_identifier_type='NoveList ID' where name='NoveList Select';
update datasources set primary_identifier_type='ISBN' where name='Content Cafe';
