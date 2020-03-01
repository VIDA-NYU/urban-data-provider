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

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.math.BigDecimal;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DecimalColumnStats extends ColumnStats {
    
    private BigDecimal _minVal = BigDecimal.ZERO;
    private BigDecimal _maxVal = BigDecimal.ZERO;
    
    public void add(BigDecimal value, int count) {
        
        super.inc(count);
        
        if (value.compareTo(_minVal) < 0) {
            _minVal = value;
        }
        if (value.compareTo(_maxVal) > 0) {
            _maxVal = value;
        }
    }

    public BigDecimal minValue() {
        
        return _minVal;
    }
    
    public BigDecimal maxValue() {
        
        return _maxVal;
    }
    
    @Override
    public JsonObject toJson() {
        
        JsonObject doc = super.toJson();
        if (this.distinctCount() > 0) {
            doc.add("minValue", new JsonPrimitive(_minVal));
            doc.add("maxValue", new JsonPrimitive(_maxVal));
        }
        return doc;
    }
}
