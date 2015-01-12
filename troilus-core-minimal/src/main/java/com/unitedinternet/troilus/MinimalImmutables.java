/*
 * Copyright (c) 2015 1&1 Internet AG, Germany, http://www.1und1.de
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


import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;



/**
 * Immutable utility class
 */
class MinimalImmutables {

    /*
     * THIS WILL BE REMOVED BY GIVING UP JAVA 7 SUPPORT
     */
    

    /**
     * merges a new entry into a set
     * 
     * @param set         the set 
     * @param entryToAdd  the entry to add
     * @return the new merged immutable set
     */
    public static <T> ImmutableSet<T> merge(ImmutableSet<T> set, T entryToAdd) {
        return ImmutableSet.<T>builder().addAll(set).add(entryToAdd).build();
    }
    
    
    /**
     * merges 2 sets
     * 
     * @param set1  the set1 to merge
     * @param set2  the set2 to merge
     * @return the new merged immutable set
     */
    public static <T> ImmutableSet<T> merge(ImmutableSet<T> set1, ImmutableSet<T> set2) {
        return ImmutableSet.<T>builder().addAll(set1).addAll(set2).build();
    }

    
    /**
     * merges a new entry into a list
     * 
     * @param list        the list to merge
     * @param entryToAdd  the entry to add
     * @return the new merged immutable list
     */
    public static <T> ImmutableList<T> merge(ImmutableList<T> list, T entryToAdd) {
        return ImmutableList.<T>builder().addAll(list).add(entryToAdd).build();
    }
 
    
    /**
     * merges 2 lists
     * 
     * @param list1  the list1 to merge
     * @param list2  the list2 to merge
     * @return the new merged immutable list
     */
    public static <T> ImmutableList<T> merge(ImmutableList<T> list1, ImmutableList<T> list2) {
        return ImmutableList.<T>builder().addAll(list1).addAll(list2).build();
    }

    
    /**
     * merges a new entry into a map
     * 
     * @param map    the map to merge
     * @param key    the key of the new entry
     * @param value  the value of the new entry
     * @return the new merged immutable map
     */
    public static <K, V> ImmutableMap<K, V> merge(ImmutableMap<K, V> map, K key, V value) {
        Map<K, V> m = Maps.newHashMap(map);
        m.put(key, value);
        return ImmutableMap.copyOf(m);
    }

   
    /**
     * merges 2 maps
     * 
     * @param map1   the map1 to merge
     * @param map2   the map2 to merge
     * @return the new merged immutable map
     */
    public static <K, V> ImmutableMap<K, V> merge(ImmutableMap<K, V> map1, ImmutableMap<K, V> map2) {
        return ImmutableMap.<K, V>builder().putAll(map1).putAll(map2).build();
    }
}
   