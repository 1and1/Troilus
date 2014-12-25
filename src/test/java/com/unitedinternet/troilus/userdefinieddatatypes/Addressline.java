package com.unitedinternet.troilus.userdefinieddatatypes;

import javax.persistence.Column;


public class Addressline {

    @Column(name = "line")
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
