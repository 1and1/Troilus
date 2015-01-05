CREATE TABLE device (
               		   deviceid text,
               		   type int,
               		   phonenumbers set<text>,
                       PRIMARY KEY (deviceid)
                    ); 