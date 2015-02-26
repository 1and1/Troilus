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
package net.oneandone.troilus.example.service;

import java.util.Optional;

import net.oneandone.troilus.Field;

import com.google.common.collect.ImmutableSet;



public class Hotel  {
   
    @Field(name = "id")
    private String id = null;
    
    @Field(name = "name")
    private String name = null;

    @Field(name = "room_ids")
    private ImmutableSet<String> roomIds = ImmutableSet.of();

    @Field(name = "classification")
    private Optional<Integer> classification = Optional.empty();
    
    @Field(name = "description")
    private Optional<String> description = Optional.empty();

    @Field(name = "picture_uri")
    private String pictureUri;
    
    @SuppressWarnings("unused")
    private Hotel() { }
    
    public Hotel(String id, 
                 String name, 
                 ImmutableSet<String> roomIds,  
                 String pictureUri,
                 Optional<Integer> classification, 
                 Optional<String> description) {
        this.id = id;
        this.name = name;
        this.roomIds = roomIds;
        this.pictureUri = pictureUri;
        this.classification = classification;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }
    
    public ImmutableSet<String> getRoomIds() {
        return roomIds;
    }
    
    public String getPictureUri() {
        return pictureUri;
    }

    public Optional<Integer> getClassification() {
        return classification;
    }

    public Optional<String> getDescription() {
        return description;
    }
}
