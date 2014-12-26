package com.unitedinternet.troilus.userdefinieddatatypes;


import com.unitedinternet.troilus.Field;


public class Addressline {

    @Field(name = "line")
    private String line;
        
    @SuppressWarnings("unused")
    private Addressline() { }

    
    public Addressline(String line) {
        this.line = line;
    }


    public String getLine() {
        return line;
    }
}
