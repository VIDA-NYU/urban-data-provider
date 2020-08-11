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
package org.urban.data.provider.socrata.cli;

import java.io.PrintWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.util.FileSystem;
import org.urban.data.core.util.SynchronizedWriter;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;
import org.urban.data.provider.socrata.db.DatasetQuery;

/**
 * Parse downloaded TSV files to ensure that all rows have the right number of
 * columns. Report any files that cannot be parsed correctly.
 * 
 * For each dataset the number of columns, total rows, and the number of
 * successfully parsed rows are reported. If all three values are -1 the dataset
 * file failed to pare at all. The output is tab-delimited with the following
 * columns:
 * 
 * 1) domain
 * 2) dataset identifier
 * 3) download date
 * 4) number of columns
 * 5) total number of rows
 * 6) number of successful parsed rows
 * 
 * Expects a given date key as argument. Will parse all downloaded files for
 * the given date.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Parse extends CommandImpl implements Command {

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
        
        private int[] parse(Dataset dataset) {

            int columnCount = -1;
            int rowCount = 0;
            int validCount = 0;

            try (CSVParser in = _db.open(dataset)) {
                columnCount = in.getHeaderNames().size();
                try {
                    for (CSVRecord record : in) {
                        if (record.size() == columnCount) {
                            validCount++;
                        }
                        rowCount++;
                    }
                } catch (java.lang.Exception ex) {
                    LOGGER.log(Level.SEVERE, dataset.toString(), ex);
                    return new int[]{-1, -1, -1};
                }
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.WARNING, dataset.toString(), ex);
            }
            return new int[]{Math.max(columnCount, 0), rowCount, validCount};
        }

        @Override
        public void run() {

            Dataset dataset;
            while ((dataset = _queue.poll()) != null) {
                int[] stats = this.parse(dataset);
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
            }
        }
    }
    
    private static final Logger LOGGER = Logger
            .getLogger(Parse.class.getName());
    

    public Parse() {

        super(
                "parse",
                "Statistics for downloaded datasets",
                "Parses downloaded dataset files. For each dataset the number of\n" +
                "columns, lines, and rows that were parsed successful is output.\n" +
                "If the dataset file fails to parse all three values are -1"
        );
        
        this.addParameter(Args.PARA_DOMAIN);
        this.addParameter(Args.PARA_DATASET);
        this.addParameter(Args.PARA_DATE, "Stats for files downloaded on this date (default: today)");
        this.addParameter(Args.PARA_OUTPUT, "Output file (default: standard output)");
        this.addParameter(Args.PARA_THREADS);
    }

    @Override
    public String name() {

        return "parse";
    }

    public void run(
            DB db,
            DatasetQuery query,
            int threads,
            SynchronizedWriter out,
            boolean printToStdOut
    ) throws java.io.IOException {
        
        ConcurrentLinkedQueue<Dataset> datasets;
        datasets = new ConcurrentLinkedQueue<>(db.getDatasets(query));
        
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
    
    @Override
    public void run(Args args) throws java.io.IOException {
        
        PrintWriter out;
        boolean printToStdOut;
        if (args.hasOutput()) {
            out = FileSystem.openPrintWriter(args.getOutput());
            printToStdOut = true;
        } else {
            out = new PrintWriter(System.out);
            printToStdOut = false;
        }
        try (SynchronizedWriter writer = new SynchronizedWriter(out)) {
            this.run(
                   args.getDB(),
                    args.asQuery(),
                    args.getThreads(),
                    writer,
                    printToStdOut
            );
        }
    }
}
