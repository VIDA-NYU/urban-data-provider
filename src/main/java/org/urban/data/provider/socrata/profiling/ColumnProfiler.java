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

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;

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
    private final TextColumnStats _textValues = new TextColumnStats();
    private final SocrataTypeChecker _typeChecker = new SocrataTypeChecker();
    
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
    
    public Value profile(String term) {
        
        return this.profile(term, 1);
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
    
    private final static String COMMAND = "Usage: <column-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(ColumnProfiler.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File columnFile = new File(args[0]);
        
        ColumnProfiler profiler = new ColumnProfiler();
        
        try (InputStream is = FileSystem.openFile(columnFile)) {
            CSVParser parser;
            parser = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
            for (CSVRecord row : parser) {
                if (row.size() == 2) {
                    String term = row.get(0);
                    int count = Integer.parseInt(row.get(1));
                    Value value = profiler.profile(term, count);
                    System.out.println(term + "\t" + value);
                } else {
                    LOGGER.log(Level.INFO, columnFile.getAbsolutePath());
                    LOGGER.log(Level.INFO, row.toString());
                }
            }
        } catch (java.lang.IllegalStateException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, columnFile.getAbsolutePath(), ex);
            System.exit(-1);
        }
    }
}
