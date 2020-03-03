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
package org.urban.data.provider.socrata.study.prepare;

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

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnStatsLoadFile {
    
    private static final String COMMAND = "Usage: <input-file> <output-file>";
    private static final Logger LOGGER = Logger
            .getLogger(ColumnStatsLoadFile.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputFile = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (
                InputStream is = FileSystem.openFile(inputFile);
                PrintWriter out = FileSystem.openPrintWriter(outputFile)
        ) {
            CSVPrinter csvPrinter = new CSVPrinter(out, CSVFormat.TDF);
            CSVParser parser;
            parser = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
            int newLineCount = 0;
            int tabCount = 0;
            for (CSVRecord row : parser) {
                String name = row.get(2);
                if (name.contains("\n")) {
                    name = name.replaceAll("\n", "\\n");
                    newLineCount++;
                }
                if (name.contains("\t")) {
                    name = name.replaceAll("\t", " ");
                    tabCount++;
                }
                csvPrinter.printRecord(
                        row.get(0),
                        row.get(1),
                        name,
                        row.get(3),
                        row.get(4),
                        row.get(5),
                        row.get(6),
                        row.get(7)
                );
            }
            System.out.println("New Lines: " + newLineCount);
            System.out.println("Tabs     : " + tabCount);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
