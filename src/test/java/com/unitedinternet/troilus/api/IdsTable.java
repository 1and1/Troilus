package com.unitedinternet.troilus.api;

import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Tables;



public interface IdsTable  {
   
    public static final String TABLE = "ids";
    
    public static final String ID = "id";
    public static final String IDS = "ids";
    
    public static final ImmutableSet<String> ALL = ImmutableSet.of(ID, IDS);
    
    public static final String CREATE_STMT = Tables.load("com/unitedinternet/troilus/example/ids.ddl");
}
