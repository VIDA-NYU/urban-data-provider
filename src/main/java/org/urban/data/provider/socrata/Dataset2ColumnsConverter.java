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
package org.urban.data.provider.socrata;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileListReader;
import org.urban.data.core.io.FileSystem;

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
public class Dataset2ColumnsConverter {
    
    private final ColumnFactory _columnFactory;
    
    public Dataset2ColumnsConverter(
            File outputDir,
            PrintWriter out,
            boolean toUpper
    ) {
        _columnFactory = new ColumnFactory(outputDir, out, toUpper);
    }
    
    /**
     * Convert a list of dataset files into a set of column files.
     * 
     * @param files
     * @throws java.io.IOException 
     */
    public void run(List<File> files) throws java.io.IOException {

        int count = 0;
        for (File file : files) {
            String dataset;
            if (file.getName().endsWith(".tsv")) {
                dataset = file.getName().substring(0, file.getName().length() - 4);
            } else if (file.getName().endsWith(".tsv.gz")) {
                dataset = file.getName().substring(0, file.getName().length() - 7);
            } else {
                return;
            }
            System.out.println((++count) + " of " + files.size() + ": " + file.getName());
            try (CSVParser in = SocrataHelper.tsvParser(file)) {
                List<ColumnHandler> columns = new ArrayList<>();
                for (String colName : in.getHeaderNames()) {
                    columns.add(_columnFactory.getHandler(dataset, colName));
                }
                for (CSVRecord row : in) {
                    for (int iColumn = 0; iColumn < row.size(); iColumn++) {
                        String term = row.get(iColumn);
                        if (!term.equals("")) {
                            columns.get(iColumn).add(term);
                        }
                    }
                }
                for (ColumnHandler column : columns) {
                    column.close();
                }
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <input-dir>\n" +
            "  <columns-file>\n" +
            "  <to-upper>\n" +
            "  <output-dir>";
    
    public static void main(String[] args) {
        
        System.out.println("Convert Datasets to Columns (Version 0.1.0)");

        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
        File columnFile = new File(args[1]);
        boolean toUpper = Boolean.parseBoolean(args[2]);
        File outputDir = new File(args[3]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(columnFile)) {
            List<File> files = new FileListReader(new String[]{".csv", ".tsv"})
                    .listFiles(inputFile);
            new Dataset2ColumnsConverter(outputDir, out, toUpper).run(files);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
