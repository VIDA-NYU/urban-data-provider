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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.urban.data.core.io.FileSystem;

/**
 * Write file size information for all downloaded dataset files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CollectDatasetFilesInfo {
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(CollectDatasetFilesInfo.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Dataset Files Info Writer - 0.1.2");
    
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        File outputFile = new File(args[1]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            CSVPrinter csvPrinter = new CSVPrinter(out, CSVFormat.TDF);
            long sum = 0;
            for (File directory : baseDir.listFiles()) {
                if (directory.isDirectory()) {
                    File datasetDir = FileSystem.joinPath(directory, "tsv");
                    if (datasetDir.isDirectory()) {
                        for (File file : datasetDir.listFiles()) {
                            String datasetName = file.getName().split("\\.")[0];
                            String size = FileSystem.humanReadableByteCount(file.length());
                            csvPrinter.printRecord(datasetName, file.length(), size);
                            csvPrinter.flush();
                            sum += file.length();
                        }
                    }
                }
            }
            String size = FileSystem.humanReadableByteCount(sum);
            csvPrinter.printRecord("TOTAL", sum, size);
            csvPrinter.flush();
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
