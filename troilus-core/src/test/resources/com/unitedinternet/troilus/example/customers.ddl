
DROP TABLE customers;

CREATE TABLE customers (
               			id text,
                    	name text,
	                    is_customer boolean,
    	                picture blob,
    	                phone_numbers set<text>,
        				current_address frozen<addr>,
        				old_addresses set<frozen<addr>>,
        				classification map<frozen<Classifier>, frozen<Score>>,
        				classification2 map<int, frozen<Score>>,
        				roles map<text, text>,
                        PRIMARY KEY (id)
                       ); 