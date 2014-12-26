package com.unitedinternet.troilus.userdefinieddatatypes;


import com.google.common.collect.ImmutableList;
import com.unitedinternet.troilus.Field;

public class Address {

    @Field(name = "lines", type = Addressline.class)
    private ImmutableList<Addressline> lines;

    @Field(name = "zip_code")
    private Integer zipCode;

    
    @SuppressWarnings("unused")
    private Address() { }

    
    public Address(ImmutableList<Addressline> lines, Integer zipCode) {
        this.lines = lines;
        this.zipCode = zipCode;
    }
    
    
    public ImmutableList<Addressline> getLines() {
        return lines;
    }


    public Integer getZipCode() {
        return zipCode;
    }    
}
