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
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DateColumnStats extends ColumnStats {
    
    private final static SimpleDateFormat DF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    
    private Date _minDate = null;
    private Date _maxDate = null;
    
    public void add(Date value, int count) {
        
        super.inc(count);
        
        if (_minDate == null) {
            _minDate = value;
        } else if (value.before(_minDate)) {
            _minDate = value;
        }
        if (_maxDate == null) {
            _maxDate = value;
        } else if (value.after(_maxDate)) {
            _maxDate = value;
        }
    }

    public Date minDate() {
        
        return _minDate;
    }
    
    public Date maxDate() {
        
        return _maxDate;
    }
    
    @Override
    public JsonObject toJson() {
        
        JsonObject doc = super.toJson();
        if (this.distinctCount() > 0) {
            doc.add("minDate", new JsonPrimitive(DF.format(_minDate)));
            doc.add("maxDate", new JsonPrimitive(DF.format(_maxDate)));
        }
        return doc;
    }
}
