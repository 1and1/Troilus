package net.oneandone.troilus.example;

import net.oneandone.troilus.Schema;


public interface RoomsTable  {
   
    public static final String TABLE = "rooms";
    
    public static final String ID = "room_id";
    public static final String HOTEL_ID = "hotel_id";
    public static final String NUMBER_OF_BEDS = "number_of_beds";
    
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/rooms.ddl");
}
