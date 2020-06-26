delete from configurationsettings where key='patron_web_hostnames';
update configurationsettings set key='patron_web_hostnames' where key='Patron Web Client';
update configurationsettings set value = value::json->>0 where key = 'patron_web_hostnames';
