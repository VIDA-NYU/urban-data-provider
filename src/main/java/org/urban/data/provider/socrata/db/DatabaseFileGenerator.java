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
package org.urban.data.provider.socrata.db;

import java.io.File;
import java.io.PrintWriter;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;

/**
 * Generate Socrata archive database file. This tool is used to re-create the
 * the archive database file in case it has been lost or corrupted. Parses the
 * folders in a given directory and checks for sub-folders that match the
 * Socrata download pattern.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatabaseFileGenerator {
    
    public void run(File inputDir, Date startDate, PrintWriter out) throws java.io.IOException {
    
        for (File file : inputDir.listFiles()) {
            if (file.isDirectory()) {
                this.traverseDomainDir(file, startDate, out);
            }
        }
    }
    
    private void traverseDomainDir(File domainDir, Date startDate, PrintWriter out) throws java.io.IOException {

        String domainName = domainDir.getName();
        System.out.println(domainName);
        for (File file : domainDir.listFiles()) {
            if (file.isDirectory()) {
                // Check if the directory has a sub-folder tsv
                File tsvFolder = FileSystem.joinPath(file, "tsv");
                if (tsvFolder.isDirectory()) {
                    try {
                        Date date = DB.DF.parse(file.getName());
                        if (!date.before(startDate)) {
                            String downloadDate = file.getName();
                            for (File datasetFile : tsvFolder.listFiles()) {
                                if (datasetFile.getName().endsWith(".tsv.gz")) {
                                    String datasetName = datasetFile.getName();
                                    datasetName = datasetName.substring(0, datasetName.length() - 7);
                                    out.println(
                                            domainName + "\t" +
                                            datasetName + "\t" +
                                            downloadDate + "\t" +
                                            DB.DOWNLOAD_SUCCESS
                                    );
                                } else {
                                    System.out.println(datasetFile.getName());
                                }
                            } 
                        } else {
                            System.out.println(file.getName());
                        }
                    } catch (java.text.ParseException ex) {
                        System.out.println(ex);
                    }
                } else {
                    System.out.println("No datasets for " + file.getName());
                }
            }
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <input-directory>\n" +
            "  <start-date>\n" +
            "  <output-file>";
    
    public static void main(String[] args) throws java.text.ParseException {
    
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputDir = new File(args[0]);
        Date startDate = DB.DF.parse(args[1]);
        File outputFile = new File(args[2]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new DatabaseFileGenerator().run(inputDir, startDate, out);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
