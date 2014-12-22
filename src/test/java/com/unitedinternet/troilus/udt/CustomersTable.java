package com.unitedinternet.troilus.udt;

import com.unitedinternet.troilus.Schema;



public interface CustomersTable  {
   
    public static final String TABLE = "customers";
    
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String ADDRES = "address";
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/customers.ddl");
 }
