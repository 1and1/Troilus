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

import com.datastax.driver.core.querybuilder.Clause;




public interface Dao extends Configurable<Dao> {

    
    InsertionWithUnit insert();

    Insertion insertObject(Object persistenceObject);

    Insertion insertValues(String name1, Object value1, String name2, Object value2);
    
    Insertion insertValues(String name1, Object value1, String name2, Object value2, String name3, Object value3);
    
    Insertion insertValues(String name1, Object value1, String name2, Object value2, String name3, Object value3, String name4, Object value4);
    
    Insertion insertValues(String name1, Object value1, String name2, Object value2, String name3, Object value3, String name4, Object value4, String name5, Object value5);


    SingleSelectionWithUnit<Optional<Record>> readWithKey(String keyName, Object keyValue);
    
    SingleSelectionWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);
    
    SingleSelectionWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);
    
    SingleSelectionWithUnit<Optional<Record>> readWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);
   
    ListSelectionWithUnit<Result<Record>> readAll();
    
    ListSelectionWithUnit<Result<Record>> readWithCondition(Clause... clauses);
    
  
    Deletion deleteWithKey(String keyName, Object keyValue);
    
    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2);
    
    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3);
    
    Deletion deleteWithKey(String keyName1, Object keyValue1, String keyName2, Object keyValue2, String keyName3, Object keyValue3, String keyName4, Object keyValue4);
}
