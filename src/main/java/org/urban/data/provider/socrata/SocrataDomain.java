/*
 * Copyright 2018 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.urban.data.provider.socrata;

import org.urban.data.core.object.NamedObject;

/**
 * Socrata domain meta data object.
 * 
 * Simply contains the domain name and the resource count.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SocrataDomain implements NamedObject {
    
    private final int _count;
    private final String _name;
    
    public SocrataDomain(String name, int count) {
        
        _name = name;
        _count = count;
    }
    
    public int count() {
        
        return _count;
    }
    
    @Override
    public String name() {
        
        return _name;
    }
}
