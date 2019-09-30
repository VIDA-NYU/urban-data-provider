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

import java.io.BufferedReader;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DB {

    // Name of the file that contains information about downloaded files
    public static final String DBFILE = "db.tsv";
    // Date format for download dates in the database file
    public static final SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd");

    private static final Logger LOGGER = Logger.getLogger(DB.class.getName());

    private final File _baseDir;
    
    public DB(File baseDir) {
        
        _baseDir = baseDir;
    }
    
    /**
     * File containing the database download information.
     * 
     * @return 
     */
    public File databaseFile() {
        
        return FileSystem.joinPath(_baseDir, DBFILE);
    }

    /**
     * File for a downloaded dataset.
     * 
     * @param dataset
     * @return 
     */
    public File datasetFile(Dataset dataset) {
        
        File file = FileSystem.joinPath(_baseDir, dataset.domain());
        file = FileSystem.joinPath(file, dataset.downloadDate());
        return FileSystem.joinPath(file, dataset.identifier() + ".tsv.gz");
    }
    

    /**
     * Read database file for a given date. Returns entries that were downloaded
     * at the given date.
     * 
     * 
     * @param date
     * @return
     * @throws java.io.IOException 
     */
    public List<Dataset> downloadedAt(String date) throws java.io.IOException {
        
        List<Dataset> db = new ArrayList<>();
        
        File file = this.databaseFile();
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String domain = tokens[0];
                    String dataset = tokens[1];
                    if (tokens[2].equals(date)) {
                        db.add(new Dataset(dataset, domain, date));
                    }
                }
            }
        }
        
        return db;
    }
    
    public List<Dataset> indexToList(HashMap<String, HashMap<String, Dataset>> db) {
        
        List<Dataset> result = new ArrayList<>();
        
        for (String domain : db.keySet()) {
            for (String dataset : db.get(domain).keySet()) {
                result.add( db.get(domain).get(dataset));
            }
        }
        
        return result;
    }
    
    /**
     * Read database file. The file is expected to be in sequential order such
     * that each file entries overrides previous entries for the same dataset.
     * 
     * @return
     * @throws java.io.IOException 
     */
    public HashMap<String, HashMap<String, Dataset>> readIndex() throws java.io.IOException {
        
        HashMap<String, HashMap<String, Dataset>> db = new HashMap<>();
        
        File file = this.databaseFile();
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String domain = tokens[0];
                    String dataset = tokens[1];
                    if (!db.containsKey(domain)) {
                        db.put(domain, new HashMap<>());
                    }
                    db.get(domain).put(
                            dataset,
                            new Dataset(dataset, domain, tokens[2])
                    );
                }
            }
        }
        
        return db;
    }
    
    /**
     * Read database file as it was on the given date. 
     * 
     * @param date
     * @return
     * @throws java.io.IOException 
     */
    public HashMap<String, HashMap<String, Dataset>> readIndex(String date) throws java.io.IOException {
        
        HashMap<String, HashMap<String, Dataset>> db = new HashMap<>();
        
        File file = this.databaseFile();
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String domain = tokens[0];
                    String dataset = tokens[1];
                    String downloadDate = tokens[2];
                    if (downloadDate.compareTo(date) <= 0) {
                        if (!db.containsKey(domain)) {
                            db.put(domain, new HashMap<>());
                        }
                        db.get(domain).put(
                                dataset,
                                new Dataset(dataset, domain, downloadDate)
                        );
                    }
                }
            }
        }
        
        return db;
    }
}
