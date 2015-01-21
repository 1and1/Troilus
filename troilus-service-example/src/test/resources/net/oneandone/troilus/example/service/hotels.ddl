CREATE TABLE hotels (
                     id text,
                     name text,
                     description text,	
                     classification int,
                     room_ids set<text>,
                     PRIMARY KEY (id)
                    );