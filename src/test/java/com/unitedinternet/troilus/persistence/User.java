package com.unitedinternet.troilus.persistence;


import java.nio.ByteBuffer;
import java.util.Optional;

import javax.persistence.Column;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class User {

    private String userId;
    
    @Column(name = "name")
    private String name;
    
    @Column(name = "is_customer")
    private Optional<Boolean> isCustomer;
    
    private Optional<ByteBuffer> picture;  
    
    private Long modified;
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


    @Column(name = "user_id")
    public String getUserId() {
        return userId;
    }


    public String getName() {
        return name;
    }


    public Optional<Boolean> isCustomer() {
        return isCustomer;
    }

    @Column(name = "picture")
    public ByteBuffer getPicture() {
        return picture.get();
    }
    
    @Column(name = "picture")
    public void setPicture(Optional<ByteBuffer> data) {
        this.picture = data;
    }

    @Column(name = "modified")
    public Optional<Long> getModified() {
        return Optional.ofNullable(modified);
    }


    @Column(name = "phone_numbers")
    public Optional<ImmutableSet<String>> getPhoneNumbers() {
        return Optional.ofNullable(phoneNumbers);
    }

    public ImmutableList<String> getAddresses() {
        return addresses;
    }
}
