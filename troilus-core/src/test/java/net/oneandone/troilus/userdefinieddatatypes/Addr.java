package net.oneandone.troilus.userdefinieddatatypes;


import net.oneandone.troilus.Field;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class Addr {

    @Field(name = "lines")
    private ImmutableList<Addressline> lines;

    @Field(name = "zip_code")
    private Integer zipCode;

    @Field(name = "aliases")
    private ImmutableMap<String, Addressline> aliases;
    
    
    @SuppressWarnings("unused")
    private Addr() { }

    
    public Addr(ImmutableList<Addressline> lines, Integer zipCode, ImmutableMap<String, Addressline> aliases) {
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
