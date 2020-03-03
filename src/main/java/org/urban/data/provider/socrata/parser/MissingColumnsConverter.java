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
package org.urban.data.provider.socrata.parser;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.provider.socrata.SocrataHelper;
import org.urban.data.provider.socrata.profiling.ColumnStats;

/**
 * Convert a set of Socrata dataset files in column files that contain the set
 * of distinct terms for each column.
 * 
 * Converts all files in the given input directory that have suffix .tsv or
 * .tsv.gz. Generates a tab-delimited columns file containing unique column
 * identifier, the column name (which is the last element in the unique path),
 * and the dataset identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MissingColumnsConverter {
    
    private static final Logger LOGGER = Logger
            .getLogger(MissingColumnsConverter.class.getName());
    
    private class Task {
        
        private final int _columnId;
        private final String _columnName;
        private final File _datasetFile;
        private final String _datasetId;
        private final String _domain;
        private final File _outputFile;
        
        public Task(File baseDir, String domain, String datasetId, int columnId, String columnName) {
            
            File domainDir = FileSystem.joinPath(baseDir, domain);
            File tsvDir = FileSystem.joinPath(domainDir, "tsv");
            File columnsDir = FileSystem.joinPath(domainDir, "columns");

            _datasetId = datasetId;
            _domain = domain;
            _datasetFile = FileSystem.joinPath(tsvDir, datasetId + ".tsv.gz");
            _outputFile = FileSystem.joinPath(columnsDir, columnId + ".txt.gz");
            _columnId = columnId;
            _columnName = columnName;
        }
        
        public int columnId() {
            
            return _columnId;
        }
        
        public String columnName() {
        
            return _columnName;
        }
        
        public File datasetFile() {
            
            return _datasetFile;
        }
        
        public File outputFile() {
            
            return _outputFile;
        }
        
        @Override
        public String toString() {
            
            return _domain + "::" + _datasetId + "::" + _columnId;
        }
    }
    
    private final List<Task> _tasks;
    
    public MissingColumnsConverter(File baseDir, File inputFile) throws java.io.IOException {

        _tasks = new ArrayList<>();
        
        try (InputStream is = FileSystem.openFile(inputFile)) {
            CSVParser parser;
            parser = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
            for (CSVRecord row : parser) {
                String domain = row.get(0);
                String datasetId = row.get(1);
                int columnId = Integer.parseInt(row.get(2));
                String name = row.get(3);
                _tasks.add(new Task(baseDir, domain, datasetId, columnId, name));
            }
        }
    }

    public void run() throws java.io.IOException {

        for (Task task : _tasks) {
            System.out.println(task);
            try (CSVParser in = SocrataHelper.tsvParser(task.datasetFile())) {
                int columnIndex = -1;
                int index = -1;
                for (String colName : in.getHeaderNames()) {
                    index++;
                    if (colName.equals(task.columnName())) {
                        if (columnIndex >= 0) {
                            LOGGER.log(Level.SEVERE, "DUPLICATE");
                            LOGGER.log(Level.SEVERE, task.toString());
                            System.exit(-1);
                        }
                        columnIndex = index;
                    }
                }
                if (columnIndex < 0) {
                    index = -1;
                    for (String colName : in.getHeaderNames()) {
                        index++;
                        if (colName.trim().equals(task.columnName())) {
                            if (columnIndex >= 0) {
                                LOGGER.log(Level.SEVERE, "DUPLICATE");
                                LOGGER.log(Level.SEVERE, task.toString());
                            }
                            columnIndex = index;
                        }
                    }
                    if (columnIndex < 0) {
                        LOGGER.log(Level.SEVERE, "MISSING");
                        LOGGER.log(Level.SEVERE, task.toString());
                        System.exit(-1);
                    }
                }
                ValueListIndex column = new ValueListIndex(
                        task.outputFile(),
                        task.columnId(),
                        task.columnName(),
                        false
                );
                for (CSVRecord row : in) {
                    String term = row.get(columnIndex);
                    if (!term.equals("")) {
                        column.add(term);
                    }
                }
                ColumnStats stats = column.write();
                System.out.println(stats);
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <base-dir>\n" +
            "  <missing-columns-file>";
    
    public static void main(String[] args) {
        
        System.out.println("Convert Missing Columns (Version 0.1.2)");

        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }

        File baseDir = new File(args[0]);
        File inputFile = new File(args[1]);
        
        
        try {
            new MissingColumnsConverter(baseDir, inputFile).run();
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
