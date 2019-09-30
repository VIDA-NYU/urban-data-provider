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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
import org.urban.data.core.io.SynchronizedWriter;

/**
 * Parse downloaded TSV files to ensure that all rows have the right number of
 * columns. Report any files that cannot be parsed correctly.
 * 
 * Expects a given date key as argument. Will parse all downloaded files for
 * the given date.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ValidateTSVFiles {

    private static SynchronizedWriter SynchronizedWriter(PrintWriter out) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private class DatasetParser implements Runnable {

        private final DB _db;
        private final boolean _printToStdOut;
        private final ConcurrentLinkedQueue<Dataset> _queue;
        private final SynchronizedWriter _out;
        
        public DatasetParser(
                DB db,
                ConcurrentLinkedQueue<Dataset> queue,
                boolean printToStdOut,
                SynchronizedWriter out
        ) {
            _db = db;
            _queue = queue;
            _printToStdOut = printToStdOut;
            _out = out;
        }
        
        private int[] parse(File file) {

            int columnCount = -1;
            int rowCount = 0;
            int validCount = 0;

            try (InputStream is = FileSystem.openFile(file)) {
                try {
                    CSVParser in = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
                    for (CSVRecord record : in) {
                        if (columnCount == -1) {
                            columnCount = record.size();
                        } else {
                            if (record.size() == columnCount) {
                                validCount++;
                            }
                            rowCount++;
                        }
                    }
                } catch (java.lang.Exception ex) {
                    LOGGER.log(Level.SEVERE, file.getName(), ex);
                    return new int[]{-1, -1, -1};
                }
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.WARNING, file.getAbsolutePath(), ex);
            }
            return new int[]{Math.max(columnCount, 0), rowCount, validCount};
        }

        @Override
        public void run() {

            Dataset dataset;
            while ((dataset = _queue.poll()) != null) {
                File file = _db.datasetFile(dataset);
                if (file.exists()) {
                    int[] stats = this.parse(file);
                    String line = dataset.domain() + "\t"
                            + dataset.identifier() + "\t"
                            + dataset.downloadDate() + "\t" 
                            + stats[0] + "\t" 
                            + stats[1] + "\t" 
                            + stats[2];
                    _out.write(line);
                    if (_printToStdOut) {
                        System.out.println(line);
                    }
                } else {
                    LOGGER.log(Level.WARNING, "Missing file {0}", file.getAbsolutePath());
                }
            }
        }
    }
    
    private static final Logger LOGGER = Logger
            .getLogger(ValidateTSVFiles.class.getName());
    
    public void run(
            DB db,
            String date,
            int threads,
            SynchronizedWriter out,
            boolean printToStdOut
    ) throws java.io.IOException {
        
        ConcurrentLinkedQueue<Dataset> datasets;
        datasets = new ConcurrentLinkedQueue<>(db.downloadedAt(date));
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(new DatasetParser(db, datasets, printToStdOut, out));
        }
        es.shutdown();
        try {
            es.awaitTermination(7, TimeUnit.DAYS);
        } catch (java.lang.InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private static final String COMMAND = 
            "Usage:\n" +
            "  <base-directory>\n" +
            "  <date>\n" +
            "  <threads>\n" +
            "  {<output-file>}";

    public static void main(String[] args) {
        
        if ((args.length < 3) || (args.length > 4)) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File baseDir = new File(args[0]);
        String date = args[1];
        int threads = Integer.parseInt(args[2]);
        File outputFile = null;
        if (args.length == 4) {
            outputFile = new File(args[3]);
        }
        
        try {
            PrintWriter out;
            boolean printToStdOut;
            if (outputFile != null) {
                out = FileSystem.openPrintWriter(outputFile);
                printToStdOut = true;
            } else {
                out = new PrintWriter(System.out);
                printToStdOut = false;
            }
            try (SynchronizedWriter writer = new SynchronizedWriter(out)) {
                new ValidateTSVFiles().run(
                        new DB(baseDir),
                        date, threads,
                        writer,
                        printToStdOut
                );
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
