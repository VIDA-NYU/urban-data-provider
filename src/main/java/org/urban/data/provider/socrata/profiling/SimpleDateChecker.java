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

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimpleDateChecker {

    private final String _delim;
    private final SimpleDateFormat _df;
    private final int[] _minLength;
    private final String _pattern;
    
    public SimpleDateChecker(String pattern, String delim, int[] minLength) {
	
        _pattern = pattern;
        _delim = delim;
        _minLength = minLength;
        _df = new SimpleDateFormat("'^'" + pattern + "'$'");
        _df.setLenient(false);
    }

    public SimpleDateChecker(String pattern, String delim) {
    
        this(pattern, delim, null);
    }

    public SimpleDateChecker(String pattern) {
    
        this(pattern, null);
    }
    
    public DateValue getValue(String value) {

        try {
            Date date = _df.parse("^" + value + "$");
            if (_delim != null) {
                String[] tokens = value.split(_delim);
                if (tokens.length <= _minLength.length) {
                    for (int iToken = 0; iToken < _minLength.length; iToken++) {
                        if (tokens[iToken].length() < _minLength[iToken]) {
                            return null;
                        }
                    }
                } else {
                    return null;
                }
            }
            return new DateValue(date);
        } catch (java.text.ParseException ex) {
            return null;
        }
    }
}
