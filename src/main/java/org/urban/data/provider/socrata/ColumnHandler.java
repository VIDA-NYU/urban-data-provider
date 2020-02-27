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
package org.urban.data.provider.socrata;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.util.count.Counter;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnHandler {
    
    private final int _columnId;
    private final String _columnName;
    private final PrintWriter _out;
    private final HashMap<String, Counter> _terms;
    private int _totalCount = 0;
    private final boolean _toUpper;
    
    public ColumnHandler(
            File file,
            int columnId,
            String columnName,
            boolean toUpper
    ) throws java.io.IOException {

        _out = FileSystem.openPrintWriter(file);
        _columnId = columnId;
        _columnName = columnName;
        _terms = new HashMap<>();
        _toUpper = toUpper;
    }

    public ColumnHandler() {

        _columnId = -1;
        _columnName = null;
        _out = null;
        _terms = null;
        _toUpper = false;
    }

    public void add(String value) {

        String term = value;
        if (_toUpper) {
            term = term.toUpperCase();
        }
        
        if (_out != null) {
            if (!_terms.containsKey(term)) {
                _terms.put(term, new Counter(1));
            } else {
                _terms.get(term).inc();
            }
        }
        _totalCount++;
    }

    public void close() {

        if (_out != null) {
            for (String key : _terms.keySet()) {
                String term = key.replaceAll("\\t", " ").replaceAll("\\n", " ");
                _out.println(term + "\t" + _terms.get(key).value());
            }
            _out.close();
        }
    }
    
    public int distinctCount() {
        
        return _terms.size();
    }
    
    public int id() {
        
        return _columnId;
    }
    
    public String name() {
        
        return _columnName;
    }
    
    public int totalCount() {
        
        return _totalCount;
    }
}
