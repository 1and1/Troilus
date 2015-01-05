CREATE TYPE addr (
                     lines list<frozen<addressline>>,
                     aliases  map<text, frozen<addressline>>,
                     zip_code int
                 )