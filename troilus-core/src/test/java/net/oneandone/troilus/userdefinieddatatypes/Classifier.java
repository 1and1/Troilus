package net.oneandone.troilus.userdefinieddatatypes;


import net.oneandone.troilus.Field;


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
    
    @Override
    public int hashCode() {
        return type.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof Classifier) {
            return ((Classifier) other).type.equals(type);
        }
        
        return false;
    }
}
