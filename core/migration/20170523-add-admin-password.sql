ALTER TABLE admins ADD COLUMN password_hashed varchar;
ALTER TABLE admins DROP COLUMN access_token;
