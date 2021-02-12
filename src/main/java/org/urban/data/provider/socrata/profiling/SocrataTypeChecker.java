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
package org.urban.data.provider.socrata.profiling;

import java.math.BigDecimal;

/**
 * Data type annotator for Socrata datasets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SocrataTypeChecker {

    private final SimpleDateChecker[] _dateCheckers;
    
    /**
     * Initialize the arrays of type checkers the default annotator uses.
     * 
     */
    public SocrataTypeChecker() {
        
        _dateCheckers = new SimpleDateChecker[]{
            new SimpleDateChecker("EEEEE, MM/dd/yyyy"),
            new SimpleDateChecker("yyyy-MM-dd'T'HH:mm:ss"),
            new SimpleDateChecker("yyyy-MM-dd'T'HH:mm:ss.SSS"),
            new SimpleDateChecker("yyyy-MM-dd", "-", new int[]{2, 1, 1}),
            new SimpleDateChecker("MM/dd/yyyy HH:mm:ss"),
            new SimpleDateChecker("yyyy/MM/dd HH:mm:ss"),
            new SimpleDateChecker("MM/dd/yyyy", "/", new int[]{1, 1, 2}),
            new SimpleDateChecker("yyyy/MM/dd", "/", new int[]{2, 1, 1}),
            new SimpleDateChecker("dd/MM/yyyy", "/", new int[]{1, 1, 2}),
            new SimpleDateChecker("MM/dd/yy", "/", new int[]{1, 1, 2}),
            new SimpleDateChecker("dd/MM/yy", "/", new int[]{1, 1, 2}),
            new SimpleDateChecker("dd-MMM-yy", "-", new int[]{2, 3, 2}),
            new SimpleDateChecker("yyyy MMM dd"),
            new SimpleDateChecker("MMM dd yyyy"),
            new SimpleDateChecker("dd MMM yyyy"),
            new SimpleDateChecker("MMM-dd"),
            new SimpleDateChecker("dd-MMM")
        };
    }

    public Value getValue(String value) {

        // First try to convert the value to an integer
        try {
            return new IntValue(Integer.parseInt(value));
        } catch (java.lang.NumberFormatException ex) {
        }
        
        // Next attempt is to convert to a long value
        try {
            return new LongValue(Long.parseLong(value));
        } catch (java.lang.NumberFormatException ex) {
        }
        
        // Next attempt is to convert to a decimal value
        try {
            return new DecimalValue(new BigDecimal(value));
        } catch (java.lang.NumberFormatException ex) {
        }
        
        // Next attempt is a Geo Point
        if (GeoPointValue.isGeoPoint(value)) {
            return new GeoPointValue(value);
        }
        
        // Check for dates
        String dateValue;
        if ((value.endsWith(" AM")) || (value.endsWith(" PM"))) {
            dateValue = value.substring(0, value.length() - 3).trim();
        } else if ((value.endsWith(" AM +0000")) || (value.endsWith(" PM +0000"))) {
            dateValue = value.substring(0, value.length() - 9).trim();
        } else {
            dateValue = value;
        }
        for (SimpleDateChecker dateChecker : _dateCheckers) {
            DateValue date = dateChecker.getValue(dateValue);
            if (date != null) {
                return date;
            }
        }
        
        return new TextValue(value);
    }
}
