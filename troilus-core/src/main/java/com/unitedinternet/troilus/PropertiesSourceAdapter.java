/*
 * Copyright (c) 2014 1&1 Internet AG, Germany, http://www.1und1.de
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.unitedinternet.troilus;

import java.util.Optional;


class PropertiesSourceAdapter implements PropertiesSource {
    
    private final Record record;
    
    public PropertiesSourceAdapter(Record record) {
        this.record = record;
    }
    
    @Override
    public <T> com.google.common.base.Optional<T> read(String name, Class<?> clazz1) {
        return read(name, clazz1, Object.class);
    }

    
    @SuppressWarnings("unchecked")
    @Override
    public <T> com.google.common.base.Optional<T> read(String name, Class<?> clazz1, Class<?> clazz2) {
        Optional<?> value = record.getObject(name, clazz1);
        if (value.isPresent()) {
            return com.google.common.base.Optional.of((T) value.get());
        } else {
            return com.google.common.base.Optional.absent();
        }
    }
}
