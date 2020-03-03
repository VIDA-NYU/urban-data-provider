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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;

/**
 * Count values and determine data types for all columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFilesValidator {
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFilesValidator.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Column Files Validator - 0.1.0");
    
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        
        for (File directory : baseDir.listFiles()) {
            if (directory.isDirectory()) {
                File columnsDir = FileSystem.joinPath(directory, "columns");
                File columnsFile = FileSystem.joinPath(directory, "columns.tsv");
                if ((columnsDir.isDirectory()) && (columnsFile.isFile())) {
                    validateDomain(directory.getName(), columnsDir, columnsFile);
                }
            }
        }
    }
    
    private static void validateDomain(String domainName, File columnsDir, File columnsFile) {
        
        try (InputStream is = FileSystem.openFile(columnsFile)) {
            CSVParser parser;
            parser = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
            for (CSVRecord row : parser) {
                int columnId = Integer.parseInt(row.get(0));
                String dataset = row.get(1);
                String filename = columnId + ".txt.gz";
                File columnFile = FileSystem.joinPath(columnsDir, filename);
                try (BufferedReader in = FileSystem.openReader(columnFile)) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        String[] tokens = line.split("\t");
                        if (tokens.length != 2) {
                            System.out.println(domainName + "\t" + columnId + "\t" + dataset);
                            break;
                        }
                    }
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, columnsFile.getAbsolutePath(), ex);
            System.exit(-1);
        }
    }
}
