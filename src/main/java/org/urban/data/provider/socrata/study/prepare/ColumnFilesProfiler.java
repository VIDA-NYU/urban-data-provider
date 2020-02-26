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
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.provider.socrata.profiling.ColumnProfiler;

/**
 * Count values and determine data types for all columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFilesProfiler {
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>";
    
    public void profile(File columnFile, int columnId, PrintWriter out) throws java.io.IOException {
        
        if (!columnFile.isFile()) {
            System.out.println("File not found " + columnFile.getAbsolutePath());
            return;
        }
        
        ColumnProfiler profiler = new ColumnProfiler("default");
        HashSet<String> upper = new HashSet<>();
        
        try (BufferedReader in = FileSystem.openReader(columnFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                String term = tokens[0];
                int count = Integer.parseInt(tokens[0]);
                profiler.add(term, count);
                String termUpper = term.toUpperCase();
                if (!upper.contains(termUpper)) {
                    upper.add(termUpper);
                }
            }
        }
        
        out.println(
                columnId + "\t" +
                profiler.distinctValues() + "\t" +
                upper.size() + "\t" +
                profiler.distinctDateValues() + "\t" +
                profiler.distinctDecimalValues() + "\t" +
                profiler.distinctIntValues() + "\t" +
                profiler.distinctLongValues() + "\t" +
                profiler.distinctTextValues()
        );
    }
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFilesProfiler.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Column Files Profiler");
    
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
                    File outputFile = FileSystem.joinPath(directory, "column-stats.tsv");
                    try (
                            BufferedReader in = FileSystem.openReader(columnsFile);
                            PrintWriter out = FileSystem.openPrintWriter(outputFile)
                        ) {
                        String line;
                        while ((line = in.readLine()) != null) {
                            String[] tokens = line.split("\t");
                            int columnId = Integer.parseInt(tokens[0]);
                            String name = tokens[1];
                            String filename = columnId + "." + name + ".txt.gz";
                            new ColumnFilesProfiler().profile(
                                    FileSystem.joinPath(columnsDir, filename),
                                    columnId,
                                    out
                            );
                        }
                    } catch (java.io.IOException ex) {
                        LOGGER.log(Level.SEVERE, "RUN", ex);
                    }
                }
            }
        }
    }
}
