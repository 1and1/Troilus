package com.unitedinternet.troilus.example;

import java.util.Optional;

import javax.persistence.Column;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;



public class Hotel  {
   
    @Column(name = "id")
    private String id = null;
    
    @Column(name = "name")
    private String name = null;

    @Column(name = "room_ids")
    private ImmutableSet<String> roomIds = ImmutableSet.of();

    @Column(name = "classification")
    private Optional<Integer> classification = Optional.empty();
    
    @Column(name = "description")
    private Optional<String> description = Optional.empty();

    
    @SuppressWarnings("unused")
    private Hotel() { }
    
    public Hotel(String id, 
                 String name, 
                 ImmutableSet<String> roomIds,  
                 Optional<Integer> classification, 
                 Optional<String> description) {
        this.id = id;
        this.name = name;
        this.roomIds = roomIds;
        this.classification = classification;
        this.description = description;
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
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("id", id)
                          .add("name", name)
                          .add("classification", classification)
                          .add("description", description)
                          .toString();
    }
}
