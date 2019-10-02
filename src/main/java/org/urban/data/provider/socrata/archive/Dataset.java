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
package org.urban.data.provider.socrata.archive;

import java.util.Date;

/**
 * Information about a downloaded dataset.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Dataset {
    
    private final String _domain;
    private final String _downloadDate;
    private final String _identifier;
    private final boolean _success;
    
    public Dataset(String identifier, String domain, String date, boolean success) {
        
        _identifier = identifier;
        _domain = domain;
        _downloadDate = date;
        _success = success;
    }
    
    public Dataset(String identifier, String domain, String date, String success) {
        
        this(identifier, domain, date, success.equals(DB.DOWNLOAD_SUCCESS));
    }

    public Dataset(String identifier, String domain, String date) {
        
        this(identifier, domain, date, true);
    }

    public String domain() {
        
        return _domain;
    }
    
    public String downloadDate() {
        
        return _downloadDate;
    }
    
    public Date getDate() {
        
        try {
            return DB.DF.parse(_downloadDate);
        } catch (java.text.ParseException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public String identifier() {
        
        return _identifier;
    }
    
    public String key() {
        
        return _domain + "#" + _identifier + "#" + _downloadDate;
    }
    
    public boolean successfulDownload() {
        
        return _success;
    }
}
