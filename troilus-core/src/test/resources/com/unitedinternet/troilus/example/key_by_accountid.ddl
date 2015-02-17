CREATE TABLE key_by_accountid (
              					account_id text,
               					key blob,
               					email_idx set<frozen<tuple<text, bigint>>>,
               					PRIMARY KEY (account_id)
                		      ); 