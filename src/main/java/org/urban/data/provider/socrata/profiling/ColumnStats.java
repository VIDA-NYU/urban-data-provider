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
 * Record of distinct and total values in a column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnStats {

    private int _decimalCount = 0;
    private boolean _hasNull = false;
    private int _intCount = 0;
    private int _longCount;
    private int _maxTextLength = 0;
    private final String _name;
    private int _textCount = 0;
    private final SocrataTypeChecker _typeChecker = new SocrataTypeChecker();
    
    public ColumnStats(String name) {
        
        _name = name;
    }
    
    public void add(String term) {
        
        if (term == null) {
            _hasNull = true;
        } else if (term.equals("")) {
            _hasNull = true;
        } else {
            Value value = _typeChecker.getValue(term);
            if (value.isInt()) {
            	_intCount++;
            } else if (value.isLong()) {
            	_longCount++;
            } else if (value.isDecimal()) {
            	_decimalCount++;
            } else {
            	_textCount++;
            }
            if (term.length() > _maxTextLength) {
                _maxTextLength = term.length();
            }
        }
    }
    
    public String name() {
        
        return _name;
    }
    
    public String sqlStmt() {
        
        String sql = _name;
        if (_textCount > 0) {
            sql += " VARCHAR(" + _maxTextLength + ")";
        } else if (_decimalCount > 0) {
            sql += " REAL";
        } else if (_longCount > 0) {
            sql += " BIGINT";
        } else if (_intCount > 0) {
            sql += " INTEGER";
        } else {
            // The column is empty.
            sql += " CHAR(1)";
        }
        if (!_hasNull) {
            sql += " NOT NULL";
        }
        return sql;
    }
}