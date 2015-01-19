package net.oneandone.troilus.userdefinieddatatypes;

import net.oneandone.troilus.Schema;



public interface AddrType  {
   
    public static final String ADDRESS_LINES = "lines";
    public static final String ZIP_CODE = "zip_code";
   
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/addr.ddl");
}
