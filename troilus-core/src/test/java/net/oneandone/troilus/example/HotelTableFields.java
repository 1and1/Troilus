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
package net.oneandone.troilus.example;


import java.util.Set;

import net.oneandone.troilus.ColumnName;



public final class HotelTableFields  {
    public static final ColumnName<String> ID = ColumnName.defineString("id");
    public static final ColumnName<String> NAME = ColumnName.defineString("name");
    public static final ColumnName<Set<String>> ROOM_IDS = ColumnName.defineSet("room_ids", String.class);
    public static final ColumnName<Address> ADDRESS = ColumnName.define("address", Address.class);
    public static final ColumnName<String> DESCRIPTION = ColumnName.defineString("description");
    public static final ColumnName<ClassifierEnum> CLASSIFICATION = ColumnName.define("classification", ClassifierEnum.class);
}
