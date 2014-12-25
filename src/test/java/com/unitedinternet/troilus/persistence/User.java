package com.unitedinternet.troilus.persistence;


import java.nio.ByteBuffer;
import java.util.Optional;



import javax.persistence.Column;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class User {

    @Column(name = "user_id")
    private String userId;
    
    @Column(name = "name")
    private String name;
 
    @Column(name = "is_customer")
    private Optional<Boolean> isCustomer;
    
    @Column(name = "picture")
    private Optional<ByteBuffer> picture;  
    
    @Column(name = "modified")
    private Long modified;
    
    @Column(name = "phone_numbers")
    private ImmutableSet<String> phoneNumbers;
    
    @Column(name = "addresses")
    private ImmutableList<String> addresses;

    
    public User() {
        
    }
    
    
    
    public User(String userId, String name, boolean isCustomer, ByteBuffer picture, long modified, ImmutableSet<String> phoneNumbers, ImmutableList<String> addresses) {
        this.userId = userId;
        this.name = name;
        this.isCustomer = Optional.of(isCustomer);
        this.picture = Optional.of(picture);
        this.modified = modified;
        this.phoneNumbers = phoneNumbers;
        this.addresses = addresses;
    }


    public String getUserId() {
        return userId;
    }


    public String getName() {
        return name;
    }


    public Optional<Boolean> isCustomer() {
        return isCustomer;
    }

    public ByteBuffer getPicture() {
        return picture.get();
    }

    public void setPicture(Optional<ByteBuffer> data) {
        this.picture = data;
    }

    public Optional<Long> getModified() {
        return Optional.ofNullable(modified);
    }

    public Optional<ImmutableSet<String>> getPhoneNumbers() {
        return Optional.ofNullable(phoneNumbers);
    }

    public ImmutableList<String> getAddresses() {
        return addresses;
    }
}
