CREATE TYPE address (
                     lines list<frozen<addressline>>,
                     zip_code int
                    )