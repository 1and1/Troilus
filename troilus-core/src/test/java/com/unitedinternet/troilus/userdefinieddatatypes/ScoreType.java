package com.unitedinternet.troilus.userdefinieddatatypes;

import com.unitedinternet.troilus.Schema;



public interface ScoreType  {
   
    public static final String TYPE = "score";
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/score.ddl");
}
