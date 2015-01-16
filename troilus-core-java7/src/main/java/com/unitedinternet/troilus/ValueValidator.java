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



 
public interface ValueValidator {
        

    /**
     * @param value  the value to validate
     * @return true if is valid
     */
    <T> boolean isValid(T value);
    
    

    public static final ValueValidator NOT_NULL = new ValueValidator() {
        
        @Override
        public <T> boolean isValid(T value) {
            return value != null;
        }
        
        public String toString() {
            return  "value cannot be null constraint";
        }
    };
    
    
    public static final ValueValidator LOWER_CASE_AND_TRIMMED_ASCII = new ValueValidator() {

        public <T> boolean isValid(T value) {
            return value instanceof String && value.equals(((String) value).toLowerCase().trim()) && isAscii((String) value);
        }

        private boolean isAscii(String value) {
            for (int i = 0; i < value.length(); i++) {
                if (value.charAt(i) > 127) {
                    return false;
                }
            }
            return true;
        }
        
        public String toString() {
            return  "value has to be lower case and ascii";
        }
    };
}