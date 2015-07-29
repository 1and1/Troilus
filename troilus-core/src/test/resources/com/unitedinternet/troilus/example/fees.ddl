
DROP TABLE fees;

CREATE TABLE fees (
                   customer_id text,
                   year int,
                   amount int,
                   PRIMARY KEY ((customer_id), year)
                  ) 