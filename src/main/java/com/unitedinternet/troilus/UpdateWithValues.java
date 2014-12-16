/*
 * Copyright (c) 2014 Gregor Roth
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

import com.google.common.collect.ImmutableMap;





/**
 * The Insertation
 *
 * @author grro
 */
public interface UpdateWithValues extends Insertion {
    
    UpdateWithValues value(String name, Object value);
    
    UpdateWithValues values(ImmutableMap<String, Object> nameValuePairsToUpdate);
    
    UpdateWithValues removeValue(String name, Object value);
    
    UpdateWithValues addSetValue(String name, Object value);
    
    UpdateWithValues removeSetValue(String name, Object value);

    UpdateWithValues appendListValue(String name, Object value);

    UpdateWithValues prependListValue(String name, Object value);

    UpdateWithValues discardListValue(String name, Object value);

    UpdateWithValues putMapValue(String name, Object key, Object value);
}


