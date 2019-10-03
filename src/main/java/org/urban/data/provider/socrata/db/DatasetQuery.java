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

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatasetQuery {
    
    private String _date = null;
    private String _domain = null;
    private String _identifier = null;
    
    public DatasetQuery dataset(String identifier) {
        
        _identifier = identifier;
        return this;
    }
    
    public DatasetQuery date(String date) {
        
        _date = date;
        return this;
    }
    
    public DatasetQuery domain(String domain) {
        
        _domain = domain;
        return this;
    }
    
    public boolean matches(Dataset ds) {
        
        if (_domain != null) {
            if (!ds.domain().equals(_domain)) {
                return false;
            }
        }
        if (_date != null) {
            if (!ds.downloadDate().equals(_date)) {
                return false;
            }
        }
        if (_identifier != null) {
            if (!ds.identifier().equals(_identifier)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * True if the dataset domain and identifier match the query and if the
     * download date is the same of before the query date.
     * 
     * @param ds
     * @return 
     */
    public boolean matchesAtOrBefore(Dataset ds) {
        
        if (_domain != null) {
            if (!ds.domain().equals(_domain)) {
                return false;
            }
        }
        if (_identifier != null) {
            if (!ds.identifier().equals(_identifier)) {
                return false;
            }
        }
        if (_date != null) {
            if (ds.downloadDate().compareTo(_date) > 0) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public String toString() {
        
        String line = "";
        
        if (_domain != null) {
            line = _domain;
        }
        if (_identifier != null) {
            line += " dataset " + _identifier;
        }
        if (_date != null) {
            line += " download on " + _date;
        }
        
        if (line.equals("")) {
            line = "all downloads";
        }
        return line.trim();
    }
}
