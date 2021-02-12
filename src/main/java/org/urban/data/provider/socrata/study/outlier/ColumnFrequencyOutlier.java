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
package org.urban.data.provider.socrata.study.outlier;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.value.ValueCounter;

/**
 * Collect information about the most frequent value for all columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFrequencyOutlier {
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFrequencyOutlier.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Frequency Outliers - 0.1.0");
    
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            CSVPrinter csvPrinter = new CSVPrinter(out, CSVFormat.TDF);
            for (File directory : baseDir.listFiles()) {
                if (directory.isDirectory()) {
                    String domain = directory.getName();
                    File columnsDir = FileSystem.joinPath(directory, "columns");
                    File columnsFile = FileSystem.joinPath(directory, "columns.tsv");
                    if (columnsFile.isFile()) {
                        try (InputStream is = FileSystem.openFile(columnsFile)) {
                            System.out.println(directory.getName());
                            CSVParser parser;
                            parser = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
                            for (CSVRecord row : parser) {
                                int columnId = Integer.parseInt(row.get(0));
                                String filename = columnId + ".txt.gz";
                                File columnFile = FileSystem.joinPath(columnsDir, filename);
                                ColumnTerms column = new ColumnTerms(columnFile);
                                ValueCounter freq = column.getMostFrequent();
                                if (freq != null) {
                                    csvPrinter.printRecord(
                                            columnId,
                                            domain,
                                            freq.getText(),
                                            freq.getCount()
                                    );
                                }
                            }
                        } catch (java.lang.IllegalStateException | java.io.IOException ex) {
                            LOGGER.log(Level.SEVERE, directory.getName(), ex);
                            System.exit(-1);
                        }
                    }
                }
            }
            csvPrinter.flush();
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}