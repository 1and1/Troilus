package com.unitedinternet.troilus.example;

import com.unitedinternet.troilus.Tables;



public interface RoomTable  {
   
    public static final String TABLE = "rooms";
    
    public static final String ID = "room_id";
    public static final String HOTEL_ID = "hotel_id";
    public static final String NUMBER_OF_BEDS = "number_of_beds";
    
    
    public static final String CREATE_STMT = Tables.load("com/unitedinternet/troilus/example/rooms.ddl");
}
