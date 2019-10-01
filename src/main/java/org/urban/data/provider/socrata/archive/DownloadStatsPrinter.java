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
package org.urban.data.provider.socrata.archive;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;

/**
 * Print total number and size of datasets that were downloaded on a given date.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DownloadStatsPrinter {
   
    public void run(DB db, String date) throws java.io.IOException {
        
        int fileCount = 0;
        long totalSize = 0;
        
        for (Dataset dataset : db.downloadedAt(date)) {
            File file = db.datasetFile(dataset);
            if (file.exists()) {
                fileCount++;
                totalSize += file.length();
            }
        }
        
        System.out.println("NUMBER OF FILES    : " + fileCount);
        System.out.println("TOTAL DOWNLOAD SIZE: " + FileSystem.humanReadableByteCount(totalSize));
    }
    
    private static final String COMMAND = 
            "Usage:\n" +
            "  <date>\n" +
            "  <databse-dir>";
    
    private static final Logger LOGGER = Logger
            .getLogger(DownloadStatsPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        String date = args[0];
        File inputDir = new File(args[1]);
        
        try {
            new DownloadStatsPrinter().run(new DB(inputDir), date);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
