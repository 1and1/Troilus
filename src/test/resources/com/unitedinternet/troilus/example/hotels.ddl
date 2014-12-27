CREATE TABLE hotels (
                     id text,
                     name text,
                     description text,	
                     classification int,
                     room_ids set<text>,
                     address frozen<address>,
                     PRIMARY KEY (id)
                    );