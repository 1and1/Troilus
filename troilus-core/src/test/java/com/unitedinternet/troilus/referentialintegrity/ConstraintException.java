package com.unitedinternet.troilus.referentialintegrity;



class ConstraintException extends RuntimeException {
    private static final long serialVersionUID = 9207265892679971373L;

    public ConstraintException(String message) {
        super(message);
    }
    
    
    public static void throwIf(boolean condition, String message) {
        if (condition) {
            throw new ConstraintException(message);
        }
    }
}



