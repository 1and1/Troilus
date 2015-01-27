/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.troilus.persistence;


import java.nio.ByteBuffer;
import java.util.Optional;




import net.oneandone.troilus.Field;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class User {

    @Field(name = "user_id")
    private String userId;
    
    @Field(name = "name")
    private String name;
 
    @Field(name = "is_customer")
    private Optional<Boolean> isCustomer;
    
    @Field(name = "picture")
    private Optional<ByteBuffer> picture;  

    @Field(name = "sec_id")
    private Optional<byte[]> secId;  
    
    @Field(name = "modified")
    private Long modified;
    
    @Field(name = "phone_numbers")
    private ImmutableSet<String> phoneNumbers;
    
    @Field(name = "addresses")
    private ImmutableList<String> addresses;

    @Field(name = "not_existing")
    private ImmutableList<String> notExisting;

    
    public User() {
        
    }
    
    
    
    public User(String userId, String name, boolean isCustomer, ByteBuffer picture, byte[] secId, long modified, ImmutableSet<String> phoneNumbers, ImmutableList<String> addresses) {
        this.userId = userId;
        this.name = name;
        this.isCustomer = Optional.of(isCustomer);
        this.picture = Optional.of(picture);
        this.secId = Optional.of(secId);
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

    public Optional<byte[]> getSecId() {
        return secId;
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
