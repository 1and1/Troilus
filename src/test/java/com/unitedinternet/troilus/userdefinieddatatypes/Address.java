package com.unitedinternet.troilus.userdefinieddatatypes;

import javax.persistence.Column;

import com.google.common.collect.ImmutableList;

public class Address {

    @Column(name = "lines")
    private ImmutableList<Addressline> lines;
    
    @Column(name = "zip_code")
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
