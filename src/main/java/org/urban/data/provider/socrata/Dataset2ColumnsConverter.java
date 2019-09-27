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
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.io.json.EscapedJsonParser;
import org.urban.data.core.io.json.JsonDatasetParser;
import org.urban.data.core.io.json.JsonPrimitiveEmitter;
import org.urban.data.core.util.count.Counter;

/**
 * Convert a set of Socrata dataset files in column files that contain the set
 * of distinct terms for each column.
 * 
 * Converts all files in the given input directory that have suffix .json or
 * .json.gz. For each unique path in the file a new column is generated.
 * 
 * Generates a tab-delimited columns file containing unique column identifier, 
 * the column name (which is the last element in the unique path), and the
 * dataset identifier.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Dataset2ColumnsConverter {
    
    public void convert(File file, ColumnFactory consumer) throws java.io.IOException, java.io.IOException {

        try (InputStream is = FileSystem.openFile(file)) {
            new JsonDatasetParser()
                    .parse(is, new EscapedJsonParser(new JsonPrimitiveEmitter(consumer)));
        }
    }

    public void run(File inputDir, File columnFile, File outputDir) throws java.lang.InterruptedException, java.io.IOException {

        // Create output directory if it does not exist
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        // Get list of input files. Considers any file with suffix .json or
        // .json.gz as input.
        List<File> files = new ArrayList<>();
        for (File file : inputDir.listFiles()) {
            if ((file.getName().endsWith(".json")) || (file.getName().endsWith(".json.gz"))) {
                files.add(file);
            }
        }
        
        Counter counter = new Counter(0);
        try (PrintWriter out = FileSystem.openPrintWriter(columnFile)) {
            for (File file : files) {
                String dataset;
                if (file.getName().endsWith(".json")) {
                    dataset = file.getName().substring(0, file.getName().length() - 5);
                } else if (file.getName().endsWith(".json.gz")) {
                    dataset = file.getName().substring(0, file.getName().length() - 8);
                } else {
                    return;
                }
                System.out.println(file.getName());
                ColumnFactory consumer;
                consumer = new ColumnFactory(dataset, counter, outputDir, out);
                this.convert(file, consumer);
                consumer.close();
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <input-dir>\n" +
            "  <columns-file>\n" +
            "  <output-dir>";
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputDir = new File(args[0]);
        File columnFile = new File(args[1]);
        File outputDir = new File(args[2]);
        
        try {
            new Dataset2ColumnsConverter().run(inputDir, columnFile, outputDir);
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
