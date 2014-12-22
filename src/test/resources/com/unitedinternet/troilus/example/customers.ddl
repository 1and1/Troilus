CREATE TABLE customers (
               			id text,
                    	name text,
	                    is_customer boolean,
    	                picture blob,
        				address frozen<address>,
                        PRIMARY KEY (id)
                       );