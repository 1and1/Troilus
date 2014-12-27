CREATE TABLE customers (
               			id text,
                    	name text,
	                    is_customer boolean,
    	                picture blob,
    	                phone_numbers set<text>,
        				current_address frozen<address>,
        				old_addresses set<frozen<address>>,
        				classification map<frozen<Classifier>, frozen<Score>>,
        				classification2 map<int, frozen<Score>>,
                        PRIMARY KEY (id)
                       ); 