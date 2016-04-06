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
package net.oneandone.troilus.udt;


import net.oneandone.troilus.Field;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class Addr {

    @Field(name = "lines")
    private ImmutableList<Addressline> lines;

    @Field(name = "zip_code")
    private Integer zipCode;

    @Field(name = "aliases")
    private ImmutableMap<String, Addressline> aliases;
    
    
    @SuppressWarnings("unused")
    private Addr() { }

    
    public Addr(ImmutableList<Addressline> lines, Integer zipCode, ImmutableMap<String, Addressline> aliases) {
        this.lines = lines;
        this.zipCode = zipCode;
        this.aliases = aliases;
    }
    
    
    public ImmutableList<Addressline> getLines() {
        return lines;
    }


    public Integer getZipCode() {
        return zipCode;
    }

    
    public ImmutableMap<String, Addressline> getAliases() {
        return aliases;
    }    
}
