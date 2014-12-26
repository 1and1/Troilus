package com.unitedinternet.troilus.userdefinieddatatypes;


import com.unitedinternet.troilus.Field;


public class Classifier {

    @Field(name = "type")
    private String type;
        
    
    @SuppressWarnings("unused")
    private Classifier() {  }
    
    public Classifier(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
