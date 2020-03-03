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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.io.SynchronizedJsonWriter;

/**
 * Count values and determine data types for all columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFilesProfiler {
   
    private static final String COMMAND =
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <threads>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFilesProfiler.class.getName());
    
    public static void main(String[] args) {
    
        System.out.println("Socrata Data Study - Column Files Profiler - 0.1.5");
    
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        File outputFile = new File(args[2]);
        
        ConcurrentLinkedQueue<ColumnFile> files = new ConcurrentLinkedQueue<>();
        
        for (File directory : baseDir.listFiles()) {
            if (directory.isDirectory()) {
                File columnsDir = FileSystem.joinPath(directory, "columns");
                File columnsFile = FileSystem.joinPath(directory, "columns.tsv");
                if ((columnsDir.isDirectory()) && (columnsFile.isFile())) {
                    try (InputStream is = FileSystem.openFile(columnsFile)) {
                        CSVParser parser;
                        parser = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
                        for (CSVRecord row : parser) {
                            int columnId = Integer.parseInt(row.get(0));
                            String dataset = row.get(1);
                            String filename = columnId + ".txt.gz";
                            ColumnFile column = new ColumnFile(
                                    columnId,
                                    dataset,
                                    FileSystem.joinPath(columnsDir, filename)
                            );
                            files.add(column);
                        }
                    } catch (java.io.IOException ex) {
                        LOGGER.log(Level.SEVERE, columnsFile.getAbsolutePath(), ex);
                        System.exit(-1);
                    }
                }
            }
        }
        
        System.out.println("Start with " + files.size() + " files");
        
        try (SynchronizedJsonWriter out = new SynchronizedJsonWriter(outputFile)) {
            ExecutorService es = Executors.newCachedThreadPool();
            for (int iThread = 0; iThread < threads; iThread++) {
                es.execute(new ProfilerTask(iThread, files, out));
            }
            es.shutdown();
            es.awaitTermination(threads, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, outputFile.getAbsolutePath(), ex);
            System.exit(-1);
        }
    }
}
