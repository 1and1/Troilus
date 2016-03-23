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
package net.oneandone.troilus;


import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;



/**
 * Immutable utility class
 */
class Immutables {

    
    /**
     * merges a new entry into a set
     * 
     * @param set         the set 
     * @param entryToAdd  the entry to add
     * @return the new merged immutable set
     */
    public static <T> ImmutableSet<T> join(ImmutableSet<T> set, T entryToAdd) {
        return ImmutableSet.<T>builder().addAll(set).add(entryToAdd).build();
    }
    
    
    /**
     * merges 2 sets
     * 
     * @param set1  the set1 to merge
     * @param set2  the set2 to merge
     * @return the new merged immutable set
     */
    public static <T> ImmutableSet<T> join(ImmutableSet<T> set1, ImmutableSet<T> set2) {
        return ImmutableSet.<T>builder().addAll(set1).addAll(set2).build();
    }

    
    /**
     * merges a new entry into a list
     * 
     * @param list        the list to merge
     * @param entryToAdd  the entry to add
     * @return the new merged immutable list
     */
    public static <T> ImmutableList<T> join(ImmutableList<T> list, T entryToAdd) {
        return ImmutableList.<T>builder().addAll(list).add(entryToAdd).build();
    }

    /**
     * merges a new entry into a list
     * 
     * @param list        the list to merge
     * @param entryToAdd  the entry to add
     * @return the new merged immutable list
     */
    public static <T> ImmutableList<T> join(T entryToAdd, ImmutableList<T> list) {
        return ImmutableList.<T>builder().add(entryToAdd).addAll(list).build();
    }
 
    /**
     * merges 2 lists
     * 
     * @param list1  the list1 to merge
     * @param list2  the list2 to merge
     * @return the new merged immutable list
     */
    public static <T> ImmutableList<T> join(ImmutableList<T> list1, ImmutableList<T> list2) {
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
    public static <K, V> ImmutableMap<K, V> join(ImmutableMap<K, V> map, K key, V value) {
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
    public static <K, V> ImmutableMap<K, V> join(ImmutableMap<K, V> map1, ImmutableMap<K, V> map2) {
        return ImmutableMap.<K, V>builder().putAll(map1).putAll(map2).build();
    }
    
    /**
     *  @param <T> the type
     *  @return a Collector that accumulates the input elements into a new immutable List
     */
    public static <T> Collector<T, ?, ImmutableList<T>> toList() {
        Supplier<ImmutableList.Builder<T>> supplier = ImmutableList.Builder::new;
        BiConsumer<ImmutableList.Builder<T>, T> accumulator = (b, v) -> b.add(v);
        BinaryOperator<ImmutableList.Builder<T>> combiner = (l, r) -> l.addAll(r.build());
        Function<ImmutableList.Builder<T>, ImmutableList<T>> finisher = ImmutableList.Builder::build;

        return Collector.of(supplier, accumulator, combiner, finisher);
    }

    /**
     *  @param <T> the type
     *  @return a Collector that accumulates the input elements into a new immutable Set
     */
    public static <T> Collector<T, ?, ImmutableSet<T>> toSet() {
        Supplier<ImmutableSet.Builder<T>> supplier = ImmutableSet.Builder::new;
        BiConsumer<ImmutableSet.Builder<T>, T> accumulator = (b, v) -> b.add(v);
        BinaryOperator<ImmutableSet.Builder<T>> combiner = (l, r) -> l.addAll(r.build());
        Function<ImmutableSet.Builder<T>, ImmutableSet<T>> finisher = ImmutableSet.Builder::build;

        return Collector.of(supplier, accumulator, combiner, finisher);
    }

    /**
     *  @param <T> the type
     *  @param <K> the key type
     *  @param <V> the value type
     *  @return a  Collector that accumulates elements into a immutable Map 

     */
    public static <T, K, V> Collector<T, ?, ImmutableMap<K, V>> toMap(Function<? super T, ? extends K> keyMapper,
                                                                      Function<? super T, ? extends V> valueMapper) {
        Supplier<ImmutableMap.Builder<K, V>> supplier = ImmutableMap.Builder::new;
        BiConsumer<ImmutableMap.Builder<K, V>, T> accumulator = (b, t) -> b.put(keyMapper.apply(t), valueMapper.apply(t));
        BinaryOperator<ImmutableMap.Builder<K, V>> combiner = (l, r) -> l.putAll(r.build());
        Function<ImmutableMap.Builder<K, V>, ImmutableMap<K, V>> finisher = ImmutableMap.Builder::build;

        return Collector.of(supplier, accumulator, combiner, finisher);
    }
}  