
DROP TABLE hotels;

CREATE TABLE hotels (
                     id text,
                     name text,
                     description text,	
                     classification text,
                     room_ids set<text>,
                     address frozen<address>,
                     PRIMARY KEY (id)
                    );