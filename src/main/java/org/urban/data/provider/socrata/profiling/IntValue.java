/*
 * Copyright 2019 New York University.
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
package org.urban.data.provider.socrata.profiling;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Column value of type text.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IntValue extends Value {

    private final int _value;
    
    public IntValue(int value) {
        
        super(DataType.Integer);
        
        _value = value;
    }
    
    @Override
    public Date getAsDate() {

        throw new RuntimeException("Cannot convert text to date");
    }

    @Override
    public BigDecimal getAsDecimal() {

        return new BigDecimal(_value);
    }

    @Override
    public int getAsInt() {

        return _value;
    }

    @Override
    public long getAsLong() {

        return _value;
    }

    @Override
    public String getAsText() {

        return Integer.toString(_value);
    }
}
