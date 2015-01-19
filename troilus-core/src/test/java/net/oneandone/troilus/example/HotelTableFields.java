package net.oneandone.troilus.example;


import java.util.Set;

import net.oneandone.troilus.Name;



public final class HotelTableFields  {
    public static final Name<String> ID = Name.defineString("id");
    public static final Name<String> NAME = Name.defineString("name");
    public static final Name<Set<String>> ROOM_IDS = Name.defineSet("room_ids", String.class);
    public static final Name<Address> ADDRESS = Name.define("address", Address.class);
    public static final Name<String> DESCRIPTION = Name.defineString("description");
    public static final Name<ClassifierEnum> CLASSIFICATION = Name.define("classification", ClassifierEnum.class);
}
