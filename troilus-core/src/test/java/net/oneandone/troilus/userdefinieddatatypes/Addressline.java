package net.oneandone.troilus.userdefinieddatatypes;


import net.oneandone.troilus.Field;


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
