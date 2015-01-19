package net.oneandone.troilus.userdefinieddatatypes;

import net.oneandone.troilus.Schema;



public interface AddresslineType  {
   
    public static final String ADDRESS_LINE = "line";
    
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/addressline.ddl");
}
