package com.unitedinternet.troilus.api;

import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Schema;



public interface FeesTable  {
   
    public static final String TABLE = "fees";
    
    public static final String CUSTOMER_ID = "customer_id";
    public static final String YEAR = "year";
    public static final String AMOUNT = "amount";
    
 
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/fees.ddl");
}
