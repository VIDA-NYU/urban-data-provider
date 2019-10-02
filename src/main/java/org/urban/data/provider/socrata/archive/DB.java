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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.util.count.Counter;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DB {

    // Name of the file that contains information about downloaded files
    public static final String DBFILE = "db.tsv";
    // Date format for download dates in the database file
    public static final SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd");

    public static final String DOWNLOAD_FAILED = "F";
    public static final String DOWNLOAD_SUCCESS = "S";
    
    private final File _baseDir;
    
    public DB(File baseDir) {
        
        _baseDir = baseDir;
    }
    
    public File catalogFile(String date) {
        
        return FileSystem.joinPath(_baseDir, "catalog." + date + ".json.gz");
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
    
    public void deleteDatasets(List<Dataset> datasets) throws java.io.IOException {
        
        HashSet<String> deleteIndex = new HashSet<>();
        for (Dataset dataset : datasets) {
            deleteIndex.add (dataset.key());
        }
        
        List<Dataset> db = new ArrayList<>();
        
        for (Dataset dataset : this.listAllDatasets()) {
            String key = dataset.key();
            if (deleteIndex.contains(key)) {
                File file = this.datasetFile(dataset);
                if (file.exists()) {
                    file.delete();
                }
            } else {
                db.add(dataset);
            }
        }
        
        try (PrintWriter out = FileSystem.openPrintWriter(this.databaseFile())) {
            for (Dataset dataset : db) {
                DB.write(dataset, out);
            }
        }
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
                        db.add(new Dataset(dataset, domain, date, tokens[3]));
                    }
                }
            }
        }
        
        return db;
    }
    
    public List<String> getDates() {
    
        HashSet<String> dates = new HashSet<>();
        File file = this.databaseFile();
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    dates.add(tokens[2]);
                }
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        
        ArrayList<String> result = new ArrayList<>(dates);
        Collections.sort(result);
        return result;
    }
    
    public HashMap<String, Integer> getDateStats() {
    
        HashMap<String, Counter> stats = new HashMap<>();
        
        File file = this.databaseFile();
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String date = tokens[2];
                    if (!stats.containsKey(date)) {
                        stats.put(date, new Counter(1));
                    } else {
                        stats.get(date).inc();
                    }
                }
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        
        HashMap<String, Integer> result = new HashMap<>();
        for (String key : stats.keySet()) {
            result.put(key, stats.get(key).value());
        }
        return result;
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

    public List<Dataset> listAllDatasets() throws java.io.IOException {
        
        List<Dataset> db = new ArrayList<>();
        
        File file = this.databaseFile();
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String domain = tokens[0];
                    String dataset = tokens[1];
                    db.add(new Dataset(dataset, domain, tokens[2], tokens[3]));
                }
            }
        }
        
        return db;
    }
    
    public CSVParser open(Dataset dataset) throws java.io.IOException {
        
        InputStream is = FileSystem.openFile(this.datasetFile(dataset));
        return new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
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
                            new Dataset(dataset, domain, tokens[2], tokens[3])
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
                                new Dataset(dataset, domain, downloadDate, tokens[3])
                        );
                    }
                }
            }
        }
        
        return db;
    }
    
    public static void write(Dataset dataset, PrintWriter out) {
        
        String state;
        if (dataset.successfulDownload()) {
            state = DB.DOWNLOAD_SUCCESS;
        } else {
            state = DB.DOWNLOAD_FAILED;
        }
        out.println(
                dataset.domain() + "\t" + 
                dataset.identifier() + "\t" + 
                dataset.downloadDate() + "\t" + 
                state
        );
    }
}
