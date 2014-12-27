CREATE TYPE address (
                     lines list<frozen<addressline>>,
                     aliases  map<text, frozen<addressline>>,
                     zip_code int
                    )