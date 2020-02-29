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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;

/**
 * Count values and determine data types for all columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CollectColumnFilesInfo {
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(CollectColumnFilesInfo.class.getName());
    
    public static void main(String[] args) {
    
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            for (File directory : baseDir.listFiles()) {
                if (directory.isDirectory()) {
                    File columnsFile = FileSystem.joinPath(directory, "columns.tsv");
                    if (columnsFile.isFile()) {
                        try (BufferedReader in = FileSystem.openReader(columnsFile)) {
                            String line;
                            while ((line = in.readLine()) != null) {
                                String[] tokens = line.split("\t");
                                String name = replaceSpecChars(tokens[2]);
                                out.println(line + "\t" + name);
                            }
                        }
                    }
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
           
    private static String replaceSpecChars(String value) {
        
        String name = value;
        name = name.replaceAll("[^\\dA-Za-z]", "_");
        while (name.contains("__")) {
            name = name.replaceAll("__", "_");
        }
        return name;
    }
}
