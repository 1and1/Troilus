package com.unitedinternet.troilus.api;

import com.google.common.collect.ImmutableSet;



public interface IdsTable  {
   
    public static final String TABLE = "ids";
    
    public static final String ID = "id";
    public static final String IDS = "ids";
    
    public static final ImmutableSet<String> ALL = ImmutableSet.of(ID, IDS);
    
    
    public static final String CREATE_STMT = "CREATE TABLE ids (" + 
                                             "                   id text, " +
                                             "                   ids map<text, int>,"+
                                             "                   PRIMARY KEY (id)" +    
                                             "                  )";

}
