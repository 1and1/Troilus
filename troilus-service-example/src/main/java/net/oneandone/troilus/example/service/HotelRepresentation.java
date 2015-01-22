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

import java.util.Set;

import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.collect.ImmutableSet;



@XmlRootElement
public class HotelRepresentation {
    
    private String id;
    
    private String name;

    private Set<String> roomIds;

    
    public HotelRepresentation(String id, 
                               String name, 
                               ImmutableSet<String> roomIds) {
        setId(id);
        setName(name);
        setRoomIds(roomIds);
    }
    

    public HotelRepresentation() {
    }


    public String getId() {
        return id;
    }


    public void setId(String id) {
        this.id = id;
    }


    public String getName() {
        return name;
    }


    public void setName(String name) {
        this.name = name;
    }


    public Set<String> getRoomIds() {
        return roomIds;
    }


    public void setRoomIds(Set<String> roomIds) {
        this.roomIds = roomIds;
    }
}