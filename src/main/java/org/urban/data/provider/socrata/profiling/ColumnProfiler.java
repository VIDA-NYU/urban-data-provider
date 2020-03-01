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

/**
 * Keep track of distinct columns values, based on their type. Maintains a
 * frequency count for each value.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnProfiler extends ColumnStats {
    
    private final DateColumnStats _dateValues = new DateColumnStats();
    private final DecimalColumnStats _decimalValues = new DecimalColumnStats();
    private final ColumnStats _geoValues = new ColumnStats();
    private final IntColumnStats _intValues = new IntColumnStats();
    private final LongColumnStats _longValues = new LongColumnStats();
    private final String _name;
    private final TextColumnStats _textValues = new TextColumnStats();
    private final SocrataTypeChecker _typeChecker = new SocrataTypeChecker();
    
    public ColumnProfiler(String name) {
        
        _name = name;
    }
    
    public ColumnProfiler() {
        
        this("");
    }
    
    public void add(Value value, int count) {
        
        super.inc(count);
        
        if (value.isDate()) {
            _dateValues.add(value.getAsDate(), count);
        } else if (value.isDecimal()) {
            _decimalValues.add(value.getAsDecimal(), count);
        } else if (value.isInt()) {
            _intValues.add(value.getAsInt(), count);
        } else if (value.isLong()) {
            _longValues.add(value.getAsLong(), count);
        } else if (value.isGeoPoint()) {
            _geoValues.inc(count);
        } else {
            _textValues.add(value.getAsText(), count);
        }
    }
    
    public Value profile(String term, int count) {
        
        Value value = _typeChecker.getValue(term);
        this.add(value, count);
        return value;
    }
    
    public void profile(String term) {
        
        this.profile(term, 1);
    }
    
    public DateColumnStats dateValues() {
        
        return _dateValues;
    }
    
    public DecimalColumnStats decimalValues() {
        
        return  _decimalValues;
    }
    
    public ColumnStats geoValues() {
        
        return _geoValues;
    }
    
    public IntColumnStats intValues() {
        
        return _intValues;
    }
    
    public LongColumnStats longValues() {
        
        return _longValues;
    }
    
    public TextColumnStats textValues() {
        
        return _textValues;
    }
}
