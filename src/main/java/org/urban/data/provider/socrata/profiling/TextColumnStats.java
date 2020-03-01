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
import java.math.MathContext;
import java.math.RoundingMode;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TextColumnStats extends ColumnStats {
    
    private int _minLen = 0;
    private int _maxLen = 0;
    private long _totalLen = 0;
    
    public TextColumnStats() {
        
        super();
    }
    
    public void add(String value, int count) {
        
        super.inc(count);
        
        int len = value.length();
        
        if (len < _minLen) {
            _minLen = len;
        }
        if (len > _maxLen) {
            _maxLen = len;
        }
        _totalLen += (long)len;
    }
    
    public BigDecimal averageLength() {
        
        if (this.distinctCount() > 0) {
            return new BigDecimal(_totalLen)
                    .divide(new BigDecimal(this.distinctCount()), MathContext.DECIMAL64)
                    .setScale(4, RoundingMode.HALF_DOWN);
        } else {
            return BigDecimal.ZERO;
        }
    }
    
    @Override
    public JsonObject toJson() {
        
        JsonObject doc = super.toJson();
        if (this.distinctCount() > 0) {
            doc.add("avgLength", new JsonPrimitive(this.averageLength()));
            doc.add("minLength", new JsonPrimitive(_minLen));
            doc.add("maxLength", new JsonPrimitive(_maxLen));
        }
        return doc;
    }
}
