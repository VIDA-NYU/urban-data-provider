/*
 * Copyright 2020 New York University.
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
package org.urban.data.provider.socrata.parser;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import org.urban.data.core.io.FileSystem;


/**
 * Handler for columns values while profiling a column and generating a unique
 * column index.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ValueListIndex implements ColumnHandler {
    
    private final int _columnId;
    private final String _columnName;
    private final File _file;
    private final ArrayList<String> _terms;
    private final boolean _toUpper;
    
    public ValueListIndex(
            File file,
            int columnId,
            String columnName,
            boolean toUpper
    ) throws java.io.IOException {

        _file = file;
        _columnId = columnId;
        _columnName = columnName;
        _terms = new ArrayList<>();
        _toUpper = toUpper;
    }

    public ValueListIndex() {

        _columnId = -1;
        _columnName = null;
        _file = null;
        _terms = null;
        _toUpper = false;
    }

    public void add(String value) {

        String term = value;
        if (_toUpper) {
            term = term.toUpperCase();
        }
        
        _terms.add(term);
    }

    public ColumnStats write() throws java.io.IOException {

        if (_file == null) {
            return new ColumnStats(0, 0);
        }
        
        Collections.sort(_terms);
        
        int distinctCount = 0;
        try (PrintWriter out = FileSystem.openPrintWriter(_file)) {
            if (!_terms.isEmpty()) {
                String term = _terms.get(0);
                int counter = 1;
                distinctCount++;
                for (int iTerm = 1; iTerm < _terms.size(); iTerm++) {
                    String nextTerm = _terms.get(iTerm);
                    if (nextTerm.equals(term)) {
                        counter++;
                    } else {
                        this.writeTerm(term, counter, out);
                        term = nextTerm;
                        counter = 1;
                        distinctCount++;
                    }
                }
                this.writeTerm(term, counter, out);
            }
        }
        
        return new ColumnStats(distinctCount, _terms.size());
    }
    
    public int id() {
        
        return _columnId;
    }
    
    public String name() {
        
        return _columnName;
    }
    
    private void writeTerm(String term, int count, PrintWriter out) {
        
        term = term.replaceAll("\\t", " ").replaceAll("\\n", " ");
        out.println(term + "\t" + count);
    }
}
