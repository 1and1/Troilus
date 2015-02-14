CREATE TABLE key_by_email (
              			    email text,
              			    created bigint,
               				key blob,
               				account_id text,
               				PRIMARY KEY ((email, created))
                		  );