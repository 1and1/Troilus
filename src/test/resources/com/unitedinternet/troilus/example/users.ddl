CREATE TABLE users (
                    user_id text,
                    name text,
                    is_customer boolean,
                    picture blob,
                    modified bigint, 
                    phone_numbers set<text>,
                    addresses list<text>,
                    PRIMARY KEY (user_id)
                   );