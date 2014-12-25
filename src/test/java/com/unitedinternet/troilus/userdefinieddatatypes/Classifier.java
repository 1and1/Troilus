package com.unitedinternet.troilus.userdefinieddatatypes;

import javax.persistence.Column;


public class Classifier {

    @Column(name = "type")
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
