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
package org.urban.data.provider.socrata.study.prepare;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.value.ValueCounter;
import org.urban.data.core.value.ValueCounterImpl;


/**
 * Transform column files that were written without using the CSV writer.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TransformColumnFiles {
    
    public void write(File file) throws java.io.IOException {

        ArrayList<ValueCounter> values = new ArrayList<>();
        try (BufferedReader in = FileSystem.openReader(file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                if (tokens.length != 2) {
                    LOGGER.log(Level.SEVERE, line);
                    LOGGER.log(Level.SEVERE, file.getAbsolutePath());
                    System.exit(-1);
                }
                values.add(new ValueCounterImpl(tokens[0], Integer.parseInt(tokens[1])));
            }
        }
        
        File outputFile = new File(file.getAbsolutePath() + ".tsv.gz");
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            CSVPrinter csv = new CSVPrinter(out, CSVFormat.TDF);
            for (ValueCounter value : values) {
                csv.printRecord(value.getText(), value.getCount());
            }
            csv.flush();
        }
        System.out.println("mv -f " + outputFile.getAbsolutePath() + " " + file.getAbsolutePath());
    }
    
    private static final String COMMAND = "Usage <column-list-file>";
    private static final Logger LOGGER = Logger
            .getLogger(TransformColumnFiles.class.getName());
    
    public static void main(String[] args) {
    
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File columnListFile = new File(args[0]);
        
        try (BufferedReader in = FileSystem.openReader(columnListFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split(" ");
                File columnFile = new File(tokens[1].trim());
                new TransformColumnFiles().write(columnFile);
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
