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

import java.io.BufferedReader;
import java.io.File;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.io.SynchronizedWriter;
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
    private final SynchronizedWriter _out;
    
    public ProfilerTask(
            int id,
            ConcurrentLinkedQueue<ColumnFile> queue,
            SynchronizedWriter out
    ) {
        _id = id;
        _queue = queue;
        _out = out;
    }
    
    public void profile(
            File columnFile,
            int columnId,
            String dataset,
            SynchronizedWriter out
    ) {
        
        if (!columnFile.isFile()) {
            System.out.println("File not found " + columnFile.getAbsolutePath());
            return;
        }
        
        ColumnProfiler profiler = new ColumnProfiler();
        HashSet<String> upper = new HashSet<>();
        
        try (BufferedReader in = FileSystem.openReader(columnFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                String term = tokens[0];
                int count = Integer.parseInt(tokens[0]);
                profiler.add(term, count);
                String termUpper = term.toUpperCase();
                if (!upper.contains(termUpper)) {
                    upper.add(termUpper);
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, dataset + " (" + columnId + ")", ex);
            return;
        }
        
        out.write(
                columnId + "\t" +
                dataset + "\t" +
                profiler.distinctValues() + "\t" +
                upper.size() + "\t" +
                profiler.distinctDateValues() + "\t" +
                profiler.distinctDecimalValues() + "\t" +
                profiler.distinctIntValues() + "\t" +
                profiler.distinctLongValues() + "\t" +
                profiler.distinctGeoValues() + "\t" +
                profiler.distinctTextValues()
        );
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
