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
package org.urban.data.provider.socrata.db;

import java.io.File;
import java.io.PrintWriter;
import org.urban.data.core.io.FileSystem;

/**
 * Synchronized writer that appends datasets to the database file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatabaseWriter implements AutoCloseable {
   
    private final PrintWriter _out;
    
    public DatabaseWriter(File file, boolean append) throws java.io.IOException {
        
        _out = FileSystem.openPrintWriter(file, append);
    }

    @Override
    public void close() {

        _out.close();
    }
    
    public synchronized void write(Dataset dataset) {
        
        String state;
        if (dataset.successfulDownload()) {
            state = DB.DOWNLOAD_SUCCESS;
        } else {
            state = DB.DOWNLOAD_FAILED;
        }
        _out.println(
                dataset.domain() + "\t" + 
                dataset.identifier() + "\t" + 
                dataset.downloadDate() + "\t" + 
                state
        );
        _out.flush();
    }
}
