


CREATE OR REPLACE FUNCTION phonenumber(phoneNum text)
 						   RETURNS NULL ON NULL INPUT
						   RETURNS text LANGUAGE javascript AS '"phoneNum";';
