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

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileListReader;
import org.urban.data.core.io.FileSystem;
import org.urban.data.provider.socrata.Dataset2ColumnsConverter;

/**
 * Generate column files for all datasets in the study.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFilesGenerator {
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFilesGenerator.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Column Files Generator");
        
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        
        for (File directory : baseDir.listFiles()) {
            if (directory.isDirectory()) {
                File tsvDir = FileSystem.joinPath(directory, "tsv");
                if (tsvDir.isDirectory()) {
                    System.out.println("Domain " + directory.getName());
                    File columnsDir = FileSystem.joinPath(directory, "columns");
                    File columnsFile = FileSystem.joinPath(directory, "columns.tsv");
                    FileSystem.createFolder(columnsDir);
                    try (PrintWriter out = FileSystem.openPrintWriter(columnsFile)) {
                        List<File> files = new FileListReader(new String[]{".tsv"})
                                .listFiles(tsvDir);
                        new Dataset2ColumnsConverter(columnsDir, out, false)
                                .run(files);
                    } catch (java.io.IOException ex) {
                        LOGGER.log(Level.SEVERE, "RUN", ex);
                    }
                }
            }
        }
    }
}
