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
 * Interface for a types column value.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class Value {
    
    public enum DataType {
        Date,
        Decimal,
        Integer,
        Long,
        Text
    };
    
    private final DataType _type;
    
    public Value(DataType type) {
        
        _type = type;
    }
    
    public abstract Date getAsDate();
    public abstract BigDecimal getAsDecimal();
    public abstract int getAsInt();
    public abstract long getAsLong();
    public abstract String getAsText();
    
    public boolean isDate() {
        
        return _type.equals(DataType.Date);
    }
    
    public boolean isDecimal() {
        
        return _type.equals(DataType.Decimal);
    }

    public boolean isInt() {
        
        return _type.equals(DataType.Integer);
    }

    public boolean isLong() {
        
        return _type.equals(DataType.Long);
    }

    public boolean isText() {
        
        return _type.equals(DataType.Text);
    }
}
