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
package org.urban.data.provider.socrata.study.outlier;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.value.ValueCounter;
import org.urban.data.core.value.ValueCounterImpl;

/**
 * Each column contains a list of terms together with their frequency.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnTerms {
   
    private final List<ValueCounter> _values;
    
    /**
     * Read column terms from file.
     * 
     * @param file
     * @throws java.io.IOException 
     */
    public ColumnTerms(File file) throws java.io.IOException {

        _values = new ArrayList<>();
        
        try (InputStream is = FileSystem.openFile(file)) {
            CSVParser parser;
            parser = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
            for (CSVRecord row : parser) {
                String term = row.get(0).toUpperCase();
                int count = Integer.parseInt(row.get(1));
                _values.add(new ValueCounterImpl(term, count));
            }
        }
        
        if (_values.isEmpty()) {
            return;
        }
        
        Collections.sort(_values, new Comparator<ValueCounter>(){
            @Override
            public int compare(ValueCounter v1, ValueCounter v2) {
                return v1.getText().compareTo(v2.getText());
            }
        });
        
        ValueCounter prev = _values.get(0);
        int index = 1;
        while ((index < _values.size())) {
            ValueCounter curr = _values.get(index);
            if (prev.getText().equals(curr.getText())) {
                prev.incCount(curr.getCount());
                _values.remove(index);
            } else {
                prev = curr;
                index++;
            }
        }
    }
    
    /**
     * Get the most frequent value in the column. If there are multiple values
     * with equal most-frequent counts the result is null.
     * 
     * @return 
     */
    public ValueCounter getMostFrequent() {
       
        if (_values.isEmpty()) {
            return null;
        }
        
        ValueCounter maxValue = _values.get(0);
        int matchCount = 0;
        
        for (int iValue = 1; iValue < _values.size(); iValue++) {
            ValueCounter value = _values.get(iValue);
            if (value.getCount() > maxValue.getCount()) {
                maxValue = value;
                matchCount = 0;
            } else if (value.getCount() == maxValue.getCount()) {
                matchCount++;
            }
        }
        
        if (matchCount == 0) {
            return maxValue;
        } else {
            return null;
        }
    }
}
