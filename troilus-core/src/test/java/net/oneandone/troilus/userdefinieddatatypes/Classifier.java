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
package net.oneandone.troilus.userdefinieddatatypes;


import net.oneandone.troilus.Field;


public class Classifier {

    @Field(name = "type")
    private String type;
        
    
    @SuppressWarnings("unused")
    private Classifier() {  }
    
    public Classifier(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
    
    @Override
    public int hashCode() {
        return type.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        if (other instanceof Classifier) {
            return ((Classifier) other).type.equals(type);
        }
        
        return false;
    }
}
