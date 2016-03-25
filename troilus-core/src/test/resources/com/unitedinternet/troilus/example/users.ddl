

DROP table users;

CREATE TABLE users (
                    user_id text,
                    user_type text,
                    name text,
                    is_customer boolean,
                    picture blob,
                    sec_id blob,
                    modified timestamp, 
                    phone_numbers set<text>,
                    addresses list<text>,
                    roles map<text, text>,
                    PRIMARY KEY (user_id)
                   );