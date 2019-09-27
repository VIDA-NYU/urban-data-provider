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
package org.urban.data.provider.socrata;

import com.google.gson.JsonPrimitive;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.io.json.JsonPrimitiveConsumer;
import org.urban.data.core.util.count.Counter;

/**
 * Factory for column files. Writes column information to given output file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFactory implements JsonPrimitiveConsumer {
   
    private class ColumnHandler {
        
        private final PrintWriter _out;
        private final HashSet<String> _terms;
        
        public ColumnHandler(File file) throws java.io.IOException {
            
            _out = FileSystem.openPrintWriter(file);
            _terms = new HashSet<>();
        }
        
        public void add(String term) {
            
            if (!_terms.contains(term)) {
                _out.println(term);
                _terms.add(term);
            }
        }
        
        public void close() {
            
            _out.close();
        }
    }
    
    private final Counter _counter;
    private final HashMap<String, ColumnHandler> _columns;
    private final String _dataset;
    private final PrintWriter _out;
    private final File _outputDir;
    
    public ColumnFactory(String dataset, Counter counter, File outputDir, PrintWriter out) {
        
        _dataset = dataset;
        _counter = counter;
        _outputDir = outputDir;
        _out = out;

        _columns = new HashMap<>();
    }

    public void close() {
    
        for (ColumnHandler column : _columns.values()) {
            column.close();
        }
        _columns.clear();
    }
    
    @Override
    public void consume(String path, JsonPrimitive element) {

        if (element.isJsonPrimitive()) {
            String term = element.getAsString().toUpperCase();
            if (!_columns.containsKey(path)) {
                int columnId = _counter.inc();
                String name = path.substring(path.lastIndexOf("/") + 1);
                name = name.replaceAll(" ", "_");
                File outputFile = FileSystem.joinPath(
                        _outputDir,
                        columnId + "." + name + ".txt.gz"
                );
                ColumnHandler column;
                try {
                    column = new ColumnHandler(outputFile);
                } catch (java.io.IOException ex) {
                    throw new RuntimeException(ex);
                }
                _columns.put(path, column);
                column.add(term);
                _out.println(columnId + "\t" + name + "\t" + _dataset);
            } else {
                _columns.get(path).add(term);
            }
        }
    }
}
