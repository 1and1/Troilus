package net.oneandone.troilus.userdefinieddatatypes;

import net.oneandone.troilus.Schema;



public interface ClassifierType  {
   
    public static final String TYPE = "type";
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/classifier.ddl");
}
