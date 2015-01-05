package com.unitedinternet.troilus.referentialintegrity;

import com.unitedinternet.troilus.Schema;



public interface PhoneToDeviceTable  {
   
    public static final String TABLE = "phone_to_device";
    
    public static final String NUMBER = "number";
    public static final String DEVICE_ID = "deviceid";
        
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/phone_to_device.ddl");
}
