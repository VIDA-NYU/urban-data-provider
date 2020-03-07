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

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;

/**
 * Generate a database load file for frequency outlier results. Ignore values 
 * that are longer than a given threshold. Remove new lines and tabs in the
 * remaining values.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class OutlierLoadFileGenerator {
    
    private final static String COMMAND = 
            "Usage:\n" +
            "  <input-file>\n" +
            "  <length-threshold> [-1 to ignore]\n" +
            "  <output-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(OutlierLoadFileGenerator.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
        int threshold = Integer.parseInt(args[1]);
        File outputFile = new File(args[2]);
        
        int maxLength = -1;
        int rowCount = 0;
        try (
                BufferedReader in = FileSystem.openReader(inputFile);
                PrintWriter out = FileSystem.openPrintWriter(outputFile)
        ) {
            CSVParser parser = new CSVParser(in, CSVFormat.TDF);
            CSVPrinter csv = new CSVPrinter(out, CSVFormat.TDF);
            for (CSVRecord row : parser) {
                int columnId = Integer.parseInt(row.get(0));
                String domain = row.get(1);
                String term = row.get(2);
                int count = Integer.parseInt(row.get(3));
                if ((threshold < 0) || (term.length() < threshold)) {
                    term = term
                            .replaceAll("\n", " ")
                            .replaceAll("\\\\", "\\\\\\\\")
                            .replaceAll("\r", " ")
                            .replaceAll("\t", " ");
                    if (term.length() > maxLength) {
                        maxLength = term.length();
                    }
                    csv.printRecord(columnId, domain, term, count);
                    rowCount++;
                }
            }
            csv.flush();
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
        
        System.out.println("NUMBER OF ROWS: " + rowCount);
        System.out.println("LONGEST TERM  : " + maxLength);
    }
}
