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

import java.io.File;

/**
 * Meta-data about a column file. Contains the column identifier, the identifier
 * for the dataset the column belongs to, and the actual column file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFile {
   
    private final int _columnId;
    private final String _datasetId;
    private final File _file;
    
    public ColumnFile(int columnId, String datasetId, File file) {
        
        _columnId = columnId;
        _datasetId = datasetId;
        _file = file;
    }
    
    public String dataset() {
        
        return _datasetId;
    }
    
    public File file() {
        
        return _file;
    }
    
    public int id() {
        
        return _columnId;
    }
}
