package com.unitedinternet.troilus.example;

import java.util.Optional;

import com.google.common.collect.ImmutableSet;
import com.unitedinternet.troilus.Field;



public class Hotel  {
   
    @Field(name = "id")
    private String id = null;
    
    @Field(name = "name")
    private String name = null;

    @Field(name = "room_ids")
    private ImmutableSet<String> roomIds = ImmutableSet.of();

    @Field(name = "classification")
    private Optional<Integer> classification = Optional.empty();
    
    @Field(name = "description")
    private Optional<String> description = Optional.empty();

    @Field(name = "address")
    private Address address;

        
    
    @SuppressWarnings("unused")
    private Hotel() { }
    
    public Hotel(String id, 
                 String name, 
                 ImmutableSet<String> roomIds,  
                 Optional<Integer> classification, 
                 Optional<String> description,
                 Address address) {
        this.id = id;
        this.name = name;
        this.roomIds = roomIds;
        this.classification = classification;
        this.description = description;
        this.address = address;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }
    
    public ImmutableSet<String> getRoomIds() {
        return roomIds;
    }

    public Optional<Integer> getClassification() {
        return classification;
    }

    public Optional<String> getDescription() {
        return description;
    }
    
    public Address getAddress() {
        return address;
    }
}
