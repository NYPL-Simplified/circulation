update configurationsettings set value=concat('["', value, '"]') where key='Patron Web Client' and value is not null;
update configurationsettings set key='patron_web_hostnames' where key='Patron Web Url';
