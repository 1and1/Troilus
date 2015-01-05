package com.unitedinternet.troilus.userdefinieddatatypes;

import com.unitedinternet.troilus.Schema;



public interface ClassifierType  {
   
    public static final String TYPE = "type";
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/classifier.ddl");
}
