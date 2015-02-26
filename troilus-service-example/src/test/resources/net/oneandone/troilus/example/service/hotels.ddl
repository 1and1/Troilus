CREATE TABLE hotels (
                     id text,
                     name text,
                     description text,	
                     classification int,
                     picture_uri text,
                     room_ids set<text>,
                     PRIMARY KEY (id)
                    );