package net.oneandone.troilus.example;

import net.oneandone.troilus.Field;


public class Address {

    @Field(name = "street")
    private String street;
    
    @Field(name = "city")
    private String city;
    
    @Field(name = "post_code")
    private String postCode;
    
        
    @SuppressWarnings("unused")
    private Address() { }

    
    public Address(String street, String city, String postCode) {
        this.street = street;
        this.city = city;
        this.postCode = postCode;
    }


    public String getStreet() {
        return street;
    }


    public String getCity() {
        return city;
    }


    public String getPostCode() {
        return postCode;
    }
}
