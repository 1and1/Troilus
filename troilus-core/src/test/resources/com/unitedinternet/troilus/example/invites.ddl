DROP TABLE invites_by_group;

CREATE TABLE invites_by_group (
  group_id text,
  invite_date timestamp,
  email_address text,
  PRIMARY KEY (group_id, invite_date, email_address)
)
WITH CLUSTERING ORDER BY (invite_date ASC);