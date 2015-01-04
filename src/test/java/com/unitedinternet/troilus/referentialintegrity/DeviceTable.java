package com.unitedinternet.troilus.referentialintegrity;

import com.unitedinternet.troilus.Schema;



public interface DeviceTable  {
   
    public static final String TABLE = "device";
    
    public static final String ID = "deviceid";
    public static final String TYPE = "type";
    public static final String PHONENUMBERS = "phonenumbers";
        
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/device.ddl");
}
