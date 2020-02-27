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
public class GeoPointValue extends Value {

    private final String _value;
    
    public GeoPointValue(String value) {
        
        super(DataType.GeoPoint);
        
        _value = value;
    }
    
    @Override
    public Date getAsDate() {

        throw new RuntimeException("Cannot convert geo point to date");
    }

    @Override
    public BigDecimal getAsDecimal() {

        throw new RuntimeException("Cannot convert geo point to decimal");
    }

    @Override
    public int getAsInt() {

        throw new RuntimeException("Cannot convert geo point to int");
    }

    @Override
    public long getAsLong() {

        throw new RuntimeException("Cannot convert geo point to long");
    }

    @Override
    public String getAsText() {

        return _value;
    }
    
    private static boolean isDouble(String value) {
        
        try {
            Double.parseDouble(value);
            return true;
        } catch (java.lang.NumberFormatException ex) {
            return false;
        }
    }
    
    public static boolean isGeoPoint(String value) {
        
        if ((value.startsWith("(")) && (value.endsWith(")"))) {
            int pos = value.indexOf(",");
            if (pos != -1) {
                String val = value.substring(1, pos);
                if (isDouble(val)) {
                    val = value.substring(pos + 1, value.length() - 1);
                    return isDouble(val);
                }
            }
        }
        return false;
    }
}
