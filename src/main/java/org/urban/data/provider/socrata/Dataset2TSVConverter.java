/*
 * Copyright 2018 New York University.
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
package org.urban.data.provider.socrata;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.io.json.CSVFileWriter;
import org.urban.data.core.io.json.EscapedJsonParser;
import org.urban.data.core.io.json.JsonDatasetParser;
import org.urban.data.core.io.json.JsonPrimitiveEmitter;
import org.urban.data.core.io.json.JsonSchemaGenerator;

/**
 * Convert a set of Socrata dataset files in JSON format into TSV format.
 * 
 * Converts all files in the given input directory that have suffix .json or
 * .json.gz into tab-delimited files in the output directory. The name of the
 * converted file is the sam as the input file but with suffix .tsv.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Dataset2TSVConverter {
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <input-dir>\n" +
            "  <threads>\n" +
            "  <output-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(Dataset2TSVConverter.class.getName());
    
    private class DatasetConverterTask implements Runnable {
        
        private final ConcurrentLinkedQueue<File> _files;
        private final File _outputDir;

        public DatasetConverterTask(ConcurrentLinkedQueue<File> files, File outputDir) {

            _files = files;
            _outputDir = outputDir;
        }

        public void convert(File file) throws java.io.IOException, java.io.IOException {

            String name;
            if (file.getName().endsWith(".json")) {
                name = file.getName().substring(0, file.getName().length() - 5);
            } else if (file.getName().endsWith(".json.gz")) {
                name = file.getName().substring(0, file.getName().length() - 8);
            } else {
                return;
            }

            InputStream is = FileSystem.openFile(file);
            /*
             * Extract the dataset schema and keep record count. We
             * only need to convert datasets that have records.
             */
            JsonSchemaGenerator schema = new JsonSchemaGenerator();
            int recordCount;
            try {
                recordCount = new JsonDatasetParser().parse(is, new EscapedJsonParser(new JsonPrimitiveEmitter(schema)));
            } catch (java.lang.Exception ex) {
                LOGGER.log(Level.SEVERE, file.getName(), ex);
                return;
            }
            if (recordCount > 0) {
                is = FileSystem.openFile(file);
                File outputFile = new File(_outputDir.getAbsolutePath() + File.separator + name + ".tsv.gz");
                try (CSVPrinter out = new CSVPrinter(new OutputStreamWriter(FileSystem.openOutputFile(outputFile)), CSVFormat.TDF)) {
                    new JsonDatasetParser().parse(
                        is,
                        new EscapedJsonParser(
                            new CSVFileWriter(out, schema.columns())
                        )
                    );
                }
                try (CSVParser in = new CSVParser(new InputStreamReader(FileSystem.openFile(outputFile)), CSVFormat.TDF)) {
                    int count = 0;
                    for (CSVRecord record : in) {
                        count++;
                        if (record.size() != schema.columns().size()) {
                            throw new java.lang.IllegalArgumentException("Unexpected number of columns for record " + in.getRecordNumber());
                        }
                    }
                    if (count != (recordCount + 1)) {
                        throw new java.lang.IllegalArgumentException("Expected " + recordCount + " records instead of " + (count - 1));
                    }
                }
            }
        }

        @Override
        public void run() {

            File file;
            while ((file = _files.poll()) != null) {
                try {
                    System.out.println(file.getName());
                    this.convert(file);
                } catch (java.io.IOException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                    System.exit(-1);
                }
            }
            System.out.println("DONE");
        }
    }

    public void run(File inputDir, int threads, File outputDir) throws java.lang.InterruptedException, java.io.IOException {
        

        // Create output directory if it does not exist
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        // Get list of input files. Considers any file with suffix .json or
        // .json.gz as input.
        ConcurrentLinkedQueue<File> files = new ConcurrentLinkedQueue<>();
        for (File file : inputDir.listFiles()) {
            if ((file.getName().endsWith(".json")) || (file.getName().endsWith(".json.gz"))) {
                files.add(file);
            }
        }
        
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(new DatasetConverterTask(files, outputDir));
        }
        es.shutdown();
        es.awaitTermination(threads, TimeUnit.DAYS);
    }
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputDir = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        File outputDir = new File(args[2]);
        
        try {
            new Dataset2TSVConverter().run(inputDir, threads, outputDir);
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "DATASET TO TSV CONVERTER", ex);
            System.exit(-1);
        }
    }
}
