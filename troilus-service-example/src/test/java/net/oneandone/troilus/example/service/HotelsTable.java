package net.oneandone.troilus.example.service;

import net.oneandone.troilus.Schema;



public interface HotelsTable  {
   
    public static final String TABLE = "hotels";
    
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String ROOM_IDS = "room_ids";
    public static final String DESCRIPTION = "description";
    public static final String CLASSIFICATION = "classification";
    
    
    public static final String CREATE_STMT = Schema.load("net/oneandone/troilus/example/service/hotels.ddl");
}
