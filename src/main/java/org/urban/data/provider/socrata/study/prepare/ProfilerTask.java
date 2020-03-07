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
package org.urban.data.provider.socrata.study.prepare;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.io.SynchronizedJsonWriter;
import org.urban.data.provider.socrata.profiling.ColumnProfiler;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ProfilerTask implements Runnable {

    private static final Logger LOGGER = Logger
            .getLogger(ProfilerTask.class.getName());
    
    private final int _id;
    private final ConcurrentLinkedQueue<ColumnFile> _queue;
    private final SynchronizedJsonWriter _out;
    
    public ProfilerTask(
            int id,
            ConcurrentLinkedQueue<ColumnFile> queue,
            SynchronizedJsonWriter out
    ) {
        _id = id;
        _queue = queue;
        _out = out;
    }
    
    public void profile(
            File columnFile,
            int columnId,
            String dataset,
            SynchronizedJsonWriter out
    ) {
        
        if (!columnFile.isFile()) {
            System.out.println("File not found " + columnFile.getAbsolutePath());
            return;
        }
        
        ColumnProfiler profiler = new ColumnProfiler();
        
        try (InputStream is = FileSystem.openFile(columnFile)) {
            CSVParser parser;
            parser = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
            for (CSVRecord row : parser) {
                if (row.size() == 2) {
                    String term = row.get(0);
                    int count = Integer.parseInt(row.get(1));
                    profiler.profile(term, count);
                } else {
                    LOGGER.log(Level.INFO, columnFile.getAbsolutePath());
                    LOGGER.log(Level.INFO, row.toString());
                }
            }
        } catch (java.lang.IllegalStateException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, dataset + " (" + columnId + ")", ex);
            LOGGER.log(Level.SEVERE, columnFile.getAbsolutePath());
            return;
        }
        
        JsonObject doc = new JsonObject();
        doc.add("columnId", new JsonPrimitive(columnId));
        doc.add("datasetId", new JsonPrimitive(dataset));
        doc.add("stats", profiler.toJson());
        
        JsonObject types = new JsonObject();
        types.add("dateValues", profiler.dateValues().toJson());
        types.add("decimalValues", profiler.decimalValues().toJson());
        types.add("geoValues", profiler.geoValues().toJson());
        types.add("intValues", profiler.intValues().toJson());
        types.add("longValues", profiler.longValues().toJson());
        types.add("textValues", profiler.textValues().toJson());
        
        doc.add("types", types);
        
        out.write(doc);
    }

    @Override
    public void run() {

        int count = 0;
        
        ColumnFile column;
        while ((column = _queue.poll()) != null) {
            this.profile(column.file(), column.id(), column.dataset(), _out);
            count += 1;
            if ((count % 100) == 0) {
                System.out.println(_id + ": " + count + " columns processed");
            }
        }
    }
}
