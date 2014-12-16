package com.unitedinternet.troilus.api;

import com.google.common.collect.ImmutableSet;



public interface FeeTable  {
   
    public static final String TABLE = "fee";
    
    public static final String CUSTOMER_ID = "customer_id";
    public static final String YEAR = "year";
    public static final String AMOUNT = "amount";
    
    public static final ImmutableSet<String> ALL = ImmutableSet.of(CUSTOMER_ID, YEAR, AMOUNT);
    
    
    
    public static final String CREATE_STMT = "CREATE TABLE fee (" + 
                                             "                   customer_id text, " +
                                             "                   year int, " +
                                             "                   amount int,"+
                                             "                   PRIMARY KEY ((customer_id), year)" +    
                                             "                  )";

}
