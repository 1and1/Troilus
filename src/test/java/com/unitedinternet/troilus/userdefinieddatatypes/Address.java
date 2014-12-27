package com.unitedinternet.troilus.userdefinieddatatypes;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.unitedinternet.troilus.Field;

public class Address {

    @Field(name = "lines", type = Addressline.class)
    private ImmutableList<Addressline> lines;

    @Field(name = "zip_code")
    private Integer zipCode;

    @Field(name = "aliases", type = String.class, type2 = Addressline.class)
    private ImmutableMap<String, Addressline> aliases;
    
    
    @SuppressWarnings("unused")
    private Address() { }

    
    public Address(ImmutableList<Addressline> lines, Integer zipCode, ImmutableMap<String, Addressline> aliases) {
        this.lines = lines;
        this.zipCode = zipCode;
        this.aliases = aliases;
    }
    
    
    public ImmutableList<Addressline> getLines() {
        return lines;
    }


    public Integer getZipCode() {
        return zipCode;
    }

    
    public ImmutableMap<String, Addressline> getAliases() {
        return aliases;
    }    
}
