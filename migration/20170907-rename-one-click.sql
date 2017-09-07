update datasources set name='RBdigital' where name='OneClick';
update externalintegrations set protocol='RBdigital' where protocol='OneClick';
update configurationsettings set value='https://api.rbdigital.com/' where key='url' and value='https://api.oneclickdigital.com/';
update configurationsettings set value='http://api.rbdigitalstage.com/' where key='url' and value='https://api.oneclickdigital.us/';
update identifiers set type='RBdigital ID' where type='OneClick ID';
