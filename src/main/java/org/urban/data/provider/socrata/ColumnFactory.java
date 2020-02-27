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

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.util.count.Counter;

/**
 * Factory for column files. Writes column information to given output file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFactory {
   
    private static final Logger LOGGER = Logger
            .getLogger(ColumnFactory.class.getName());
    
    private final Counter _counter;
    private final File _outputDir;
    private final boolean _toUpper;
    
    public ColumnFactory(File outputDir, boolean toUpper) {
        
        _outputDir = outputDir;
        _toUpper = toUpper;

        _counter = new Counter(0);

        // Create output directory if it does not exist
        FileSystem.createFolder(outputDir);
    }
    
    public ColumnHandler getHandler(String dataset, String columnName) {

        int columnId = _counter.inc();
        String name = columnName.replaceAll("[^\\dA-Za-z]", "_");
        File outputFile = FileSystem.joinPath(
                _outputDir,
                columnId + ".txt.gz"
        );
        try {
            ColumnHandler handler = new ColumnHandler(
                    outputFile,
                    columnId,
                    columnName,
                    _toUpper
            );
            return handler;
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, name, ex);
            return new ColumnHandler();
        }
    }
}
