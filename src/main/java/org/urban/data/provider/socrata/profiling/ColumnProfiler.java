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
import java.util.HashMap;
import org.urban.data.core.util.count.Counter;

/**
 * Keep track of distinct columns values, based on their type. Maintains a
 * frequency count for each values.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnProfiler {
    
    private final HashMap<Long, Counter> _dateValues = new HashMap<>();
    private final HashMap<BigDecimal, Counter> _decimalValues = new HashMap<>();
    private int _emptyCells = 0;
    private final HashMap<Integer, Counter> _intValues = new HashMap<>();
    private final HashMap<Long, Counter> _longValues = new HashMap<>();
    private final String _name;
    private final HashMap<String, Counter> _textValues = new HashMap<>();
    private final SocrataTypeChecker _typeChecker = new SocrataTypeChecker();
    
    public ColumnProfiler(String name) {
        
        _name = name;
    }
    
    public void add(String term) {
        
        if (term == null) {
            _emptyCells++;
        } else if (term.equals("")) {
            _emptyCells++;
        } else {
            Value value = _typeChecker.getValue(term);
            if (value.isDate()) {
                Long key = value.getAsLong();
                if (_dateValues.containsKey(key)) {
                    _dateValues.get(key).inc();
                } else {
                    _dateValues.put(key, new Counter(1));
                }
            } else if (value.isDecimal()) {
                BigDecimal key = value.getAsDecimal();
                if (_decimalValues.containsKey(key)) {
                    _decimalValues.get(key).inc();
                } else {
                    _decimalValues.put(key, new Counter(1));
                }
            } else if (value.isInt()) {
                Integer key = value.getAsInt();
                if (_intValues.containsKey(key)) {
                    _intValues.get(key).inc();
                } else {
                    _intValues.put(key, new Counter(1));
                }
            } else if (value.isLong()) {
                Long key = value.getAsLong();
                if (_longValues.containsKey(key)) {
                    _longValues.get(key).inc();
                } else {
                    _longValues.put(key, new Counter(1));
                }
            } else {
                if (_textValues.containsKey(term)) {
                    _textValues.get(term).inc();
                } else {
                    _textValues.put(term, new Counter(1));
                }
            }
        }
    }
    
    public int distinctDateValues() {
        
        return _dateValues.size();
    }
    
    public int distinctDecimalValues() {
        
        return  _decimalValues.size();
    }
    
    public int distinctIntValues() {
        
        return _intValues.size();
    }
    
    public int distinctLongValues() {
        
        return _longValues.size();
    }
    
    public int distinctTextValues() {
        
        return _textValues.size();
    }
    
    public int distinctValues() {
        
        int result = 0;
        result += this.distinctDateValues();
        result += this.distinctDecimalValues();
        result += this.distinctIntValues();
        result += this.distinctLongValues();
        result += this.distinctTextValues();
        return result;
    }
    
    public int emptyCells() {
        
        return _emptyCells;
    }
    
    public String name() {
        
        return _name;
    }
    
    public int nonEmptyCells() {
        
        int result = 0;
        for (Counter count : _dateValues.values()) {
            result += count.value();
        }
        for (Counter count : _decimalValues.values()) {
            result += count.value();
        }
        for (Counter count : _intValues.values()) {
            result += count.value();
        }
        for (Counter count : _longValues.values()) {
            result += count.value();
        }
        for (Counter count : _textValues.values()) {
            result += count.value();
        }
        return result;
    }
}
