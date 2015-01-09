package com.unitedinternet.troilus.example;

import com.unitedinternet.troilus.Name;
import com.unitedinternet.troilus.SetName;



public final class HotelTableFields  {
   
    public static final Name<String> ID = Name.defineString("id");
    public static final Name<String> NAME = Name.defineString("name");
    public static final SetName<String> ROOM_IDS = SetName.defineString("room_ids");
    public static final Name<Address> ADDRESS = Name.define("address", Address.class);
    public static final Name<String> DESCRIPTION = Name.defineString("description");
    public static final Name<Integer> CLASSIFICATION = Name.defineInt("classification");
}
