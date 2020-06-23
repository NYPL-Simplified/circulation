update configurationsettings set value=concat('["', value, '"]') where key='Patron Web Client' and value is not null;
