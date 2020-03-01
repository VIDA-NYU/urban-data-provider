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
import java.io.BufferedWriter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.urban.data.core.io.FileListReader;
import org.urban.data.core.io.FileSystem;
import org.urban.data.provider.socrata.parser.Dataset2ColumnsConverter;

/**
 * Generate column files for all datasets in the study.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFilesGenerator {
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <domain-names-file>\n" +
            "  <run-index>\n" +
            "  <step>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFilesGenerator.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Column Files Generator - 0.1.7");
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        File domainNamesFile = new File(args[1]);
        int runIndex = Integer.parseInt(args[2]);
        int step = Integer.parseInt(args[3]);
        
        List<String> domains = new ArrayList<>();
        try (BufferedReader in = FileSystem.openReader(domainNamesFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                domains.add(line);
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "LOAD", ex);
            System.exit(-1);
        }
        
        int domainCount = 0;
        for (int iDomain = runIndex; iDomain < domains.size(); iDomain += step) {
            String domain = domains.get(iDomain);
            File domainDir = FileSystem.joinPath(baseDir, domain);
            if (domainDir.isDirectory()) {
                File tsvDir = FileSystem.joinPath(domainDir, "tsv");
                if (tsvDir.isDirectory()) {
                    System.out.println("Domain " + domain + " (" + (++domainCount + ")"));
                    File columnsDir = FileSystem.joinPath(domainDir, "columns");
                    File columnsFile = FileSystem.joinPath(domainDir, "columns.tsv");
                    createEmptyFolder(columnsDir);
                    try (BufferedWriter out = FileSystem.openBufferedWriter(columnsFile)) {
                        CSVPrinter csvPrinter = new CSVPrinter(out, CSVFormat.TDF);
                        List<File> files = new FileListReader(new String[]{".tsv"})
                                .listFiles(tsvDir);
                        new Dataset2ColumnsConverter(columnsDir, csvPrinter, false)
                                .run(files);
                        csvPrinter.flush();
                    } catch (java.io.IOException ex) {
                        LOGGER.log(Level.SEVERE, "RUN", ex);
                    }
                }
            }
        }
    }
    
    private static void createEmptyFolder(File dir) {
        
        if (!dir.exists()) {
            dir.mkdirs();
        } else {
            for (File file : dir.listFiles()) {
                file.delete();
            }
        }
    }
}
