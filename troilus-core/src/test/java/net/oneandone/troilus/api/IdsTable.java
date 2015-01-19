package net.oneandone.troilus.api;

import net.oneandone.troilus.Schema;

import com.google.common.collect.ImmutableSet;



public interface IdsTable  {
   
    public static final String TABLE = "ids";
    
    public static final String ID = "id";
    public static final String IDS = "ids";
    
    public static final ImmutableSet<String> ALL = ImmutableSet.of(ID, IDS);
    
    public static final String CREATE_STMT = Schema.load("com/unitedinternet/troilus/example/ids.ddl");
}
