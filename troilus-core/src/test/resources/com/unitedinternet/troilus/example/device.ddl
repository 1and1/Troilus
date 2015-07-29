
DROP TABLE device;

CREATE TABLE device (
               		   device_id text,
               		   type int,
               		   phone_numbers set<text>,
                       PRIMARY KEY (device_id)
                    ); 