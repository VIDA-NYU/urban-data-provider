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
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.io.SynchronizedWriter;
import org.urban.data.core.set.StringSet;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;
import org.urban.data.provider.socrata.db.DatasetQuery;

/**
 * Parse downloaded TSV files to identify columns that contain a given list of
 * values. Takes a comma-separated list of values. Prints the dataset and the
 * column name for each column that contains all the given values.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFinder extends CommandImpl implements Command {

    private class DatasetParser implements Runnable {

        private final DB _db;
        private final boolean _printToStdOut;
        private final ConcurrentLinkedQueue<Dataset> _queue;
        private final SynchronizedWriter _out;
        private final StringSet _values;
        
        public DatasetParser(
                DB db,
                ConcurrentLinkedQueue<Dataset> queue,
                StringSet values,
                boolean printToStdOut,
                SynchronizedWriter out
        ) {
            _db = db;
            _queue = queue;
            _values = values;
            _printToStdOut = printToStdOut;
            _out = out;
        }
        
        private void parse(Dataset dataset) {

            HashMap<String, StringSet> matches = new HashMap<>();
            
            try (CSVParser in = _db.open(dataset)) {
                for (String colName : in.getHeaderNames()) {
                    matches.put(colName, new StringSet());
                }
                try {
                    for (CSVRecord record : in) {
                        for (String colName : matches.keySet()) {
                            String term = record.get(colName);
                            if (_values.contains(term)) {
                                StringSet colMatches = matches.get(colName);
                                if (!colMatches.contains(term)) {
                                    colMatches.add(term);
                                }
                            }
                        }
                    }
                } catch (java.lang.Exception ex) {
                    LOGGER.log(Level.SEVERE, dataset.toString(), ex);
                    return;
                }
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.WARNING, dataset.toString(), ex);
            }
            
            for (String colName : matches.keySet()) {
                StringSet colMatches = matches.get(colName);
                if (colMatches.length() == _values.length()) {
                    String line = dataset + "\t" + colName;
                    _out.write(line);
                if (_printToStdOut) {
                    System.out.println(line);
                }
                }
            }
        }

        @Override
        public void run() {

            Dataset dataset;
            while ((dataset = _queue.poll()) != null) {
                this.parse(dataset);
            }
        }
    }
    
    private static final Logger LOGGER = Logger
            .getLogger(Parse.class.getName());
    

    public ColumnFinder() {

        super(
                "find columns",
                "Find columns that contain a given set of values",
                "Parses downloaded dataset files. Outputs the dataset and column name\n" +
                "for each column that contains all given values."
        );
        
        this.addParameter(Args.PARA_DOMAIN);
        this.addParameter(Args.PARA_DATASET);
        this.addParameter(Args.PARA_DATE, "Parse files downloaded on this date (default: last download date)");
        this.addParameter(Args.PARA_OUTPUT, "Output file (default: standard output)");
        this.addParameter(Args.PARA_THREADS);
        this.addParameter(Args.PARA_VALUES, "Comma-separated list of query terms");
    }

    public void run(
            DB db,
            DatasetQuery query,
            StringSet values,
            int threads,
            SynchronizedWriter out,
            boolean printToStdOut
    ) throws java.io.IOException {
        
        ConcurrentLinkedQueue<Dataset> datasets;
        datasets = new ConcurrentLinkedQueue<>(db.getDatasets(query));
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(
                    new DatasetParser(db, datasets, values, printToStdOut, out)
            );
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
        
        if (!args.hasValues()) {
            throw new IllegalArgumentException("Query terms are missing");
        }
        
        PrintWriter out;
        boolean printToStdOut;
        if (args.hasOutput()) {
            out = FileSystem.openPrintWriter(args.getOutput());
            printToStdOut = true;
        } else {
            out = new PrintWriter(System.out);
            printToStdOut = false;
        }

        String date = args.getDateDefaultLast();
        DatasetQuery query = args.asQuery();
        if (!args.hasDate()) {
            query = query.date(date);
        }
        try (SynchronizedWriter writer = new SynchronizedWriter(out)) {
            this.run(
                   args.getDB(),
                    query,
                    args.getValues(),
                    args.getThreads(),
                    writer,
                    printToStdOut
            );
        }
    }
}
