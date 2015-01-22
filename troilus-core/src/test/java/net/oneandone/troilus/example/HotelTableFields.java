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

import net.oneandone.troilus.Name;



public final class HotelTableFields  {
    public static final Name<String> ID = Name.defineString("id");
    public static final Name<String> NAME = Name.defineString("name");
    public static final Name<Set<String>> ROOM_IDS = Name.defineSet("room_ids", String.class);
    public static final Name<Address> ADDRESS = Name.define("address", Address.class);
    public static final Name<String> DESCRIPTION = Name.defineString("description");
    public static final Name<ClassifierEnum> CLASSIFICATION = Name.define("classification", ClassifierEnum.class);
}
