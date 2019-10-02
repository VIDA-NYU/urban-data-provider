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
import org.urban.data.provider.socrata.cli.Args;
import org.urban.data.provider.socrata.cli.Command;
import org.urban.data.provider.socrata.cli.Help;

/**
 * Print total number and size of datasets that were downloaded on a given date.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DiskUsageStatsPrinter implements Command {
   
    private class DiskUsageStats {
    
        private int _fileCount = 0;
        private long _totalSize = 0;
        
        public void add(File file) {
            
            if (file.exists()) {
                _fileCount++;
                _totalSize += file.length();
            }
        }
        
        public int fileCount() {
            
            return _fileCount;
        }
        
        public long totalSize() {
            
            return _totalSize;
        }
    }

    @Override
    public void help() {

        Help.printName(this.name(), "Disk usage");
        Help.printDir();
        Help.printDate("Download date (default: all)");
    }

    @Override
    public String name() {

        return "du";
    }
    
    private DiskUsageStats run(DB db, String date) throws java.io.IOException {
        
        DiskUsageStats stats = new DiskUsageStats();
        
        for (Dataset dataset : db.downloadedAt(date)) {
            stats.add(db.datasetFile(dataset));
        }
        
        String size = FileSystem.humanReadableByteCount(stats.totalSize());
        System.out.println("-- Disk usage for files downloaded on " + date + " --");
        System.out.println("   Number of files     : " + stats.fileCount());
        System.out.println("   Total size on disk  : " + size);
        System.out.println("-------------------------------------------------");
        
        return stats;
    }

    @Override
    public void run(Args args) throws java.io.IOException {
        
        DB db = args.getDB();
        
        if (!args.hasDate()) {
            // If the date was not given explicitly terate over all of the
            // available dates
            int fileCount = 0;
            long totalSize = 0;
            for (String date : db.getDates()) {
                DiskUsageStats stats = this.run(db, date);
                fileCount += stats.fileCount();
                totalSize += stats.totalSize();
            }
            String size = FileSystem.humanReadableByteCount(totalSize);
            System.out.println("-- Disk usage for all downloaded files ----------");
            System.out.println("   Number of files     : " + fileCount);
            System.out.println("   Total size on disk  : " + size);
            System.out.println("-------------------------------------------------");
        } else {
            this.run(db, args.getDate());
        }
    }
    
    private static final String COMMAND = 
            "Usage:\n" +
            "  <date>\n" +
            "  <databse-dir>";
    
    private static final Logger LOGGER = Logger
            .getLogger(DiskUsageStatsPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        String date = args[0];
        File inputDir = new File(args[1]);
        
        try {
            new DiskUsageStatsPrinter().run(new DB(inputDir), date);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
