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
package net.oneandone.troilus.mapping;


import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Optional;





import javax.persistence.Column;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class JPAUser {

    @Column(name = "user_id")
    private String userId;
    
    @Column(name = "name")
    private String name;
 
    @Column(name = "is_customer")
    private Optional<Boolean> isCustomer;
    
    @Column(name = "picture")
    private Optional<ByteBuffer> picture;  
    
    @Column(name = "modified")
    private Date modified;
    
    @Column(name = "phone_numbers")
    private ImmutableSet<String> phoneNumbers;
    
    @Column(name = "addresses")
    private ImmutableList<String> addresses;

    
    public JPAUser() {
        
    }
    
    
    
    public JPAUser(String userId, String name, boolean isCustomer, ByteBuffer picture, Date modified, ImmutableSet<String> phoneNumbers, ImmutableList<String> addresses) {
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

    public Optional<Date> getModified() {
        return Optional.ofNullable(modified);
    }

    public Optional<ImmutableSet<String>> getPhoneNumbers() {
        return Optional.ofNullable(phoneNumbers);
    }

    public ImmutableList<String> getAddresses() {
        return addresses;
    }
}
