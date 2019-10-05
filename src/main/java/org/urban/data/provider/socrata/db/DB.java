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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.util.count.Counter;

/**
 * Socrata dataset archive interface. Defines the storage location for various
 * data and statistic files. Maintains the index of downloaded datasets and
 * provides access to these files.
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
    
    /**
     * Get downloaded catalog file for a given date. All catalog files are
     * maintained within a special folder api.socrata.com. Similar to datasets
     * the catalogs are maintained in folders that are named by the download
     * date.
     * 
     * @param date
     * @return 
     */
    public File catalogFile(String date) {
        
        return FileSystem.joinPath(
                _baseDir, new String[]{
                    "api.socrata.com",
                    date,
                    "catalog.json.gz"
                }
        );
    }
    
    /**
     * File containing the database download information.
     * 
     * @return 
     */
    private File databaseFile() {
        
        return FileSystem.joinPath(_baseDir, DBFILE);
    }

    /**
     * File for a downloaded dataset. Datasets are stored in a subfolder tsv
     * within a directory that is named after the domain and the download date.
     * 
     * @param dataset
     * @return 
     */
    public File datasetFile(Dataset dataset) {
        
        return FileSystem.joinPath(
                _baseDir,
                new String[]{
                    dataset.domain(),
                    dataset.downloadDate(),
                    "tsv",
                    dataset.identifier() + ".tsv.gz"
                }
        );
    }

    /**
     * Get dataset file.
     * 
     * @param identifier
     * @param domain
     * @param date
     * @return 
     */
    public File datasetFile(String identifier, String domain, String date) {
        
        return this.datasetFile(new Dataset(identifier, domain, date));
    }
    
    public void deleteDatasets(List<Dataset> datasets) throws java.io.IOException {
        
        HashSet<String> deleteIndex = new HashSet<>();
        for (Dataset dataset : datasets) {
            deleteIndex.add (dataset.key());
        }
        
        List<Dataset> db = new ArrayList<>();
        
        for (Dataset dataset : this.getDatasets()) {
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
        
        try (DatabaseWriter writer = this.writer(false)) {
            for (Dataset dataset : db) {
                writer.write(dataset);
            }
        }
    }
    
    public List<String> downloadDates() {
    
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
    
    public HashMap<String, Integer> downloadDateStats() {
    
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

    /**
     * Get a snapshot of datasets that have been downloaded at the given date.
     * The result will contain an entry for the latest download of every dataset
     * that matches the domain and dataset identifier in the query and that was
     * downloaded at or before the given date. If no date is given in the query
     * the latest download for every matching dataset will be included in the
     * result.
     * 
     * @param query
     * @return
     * @throws java.io.IOException 
     */
    public List<Dataset> getSnapshot(DatasetQuery query) throws java.io.IOException {
        
        HashMap<String, HashMap<String, Dataset>> db = new HashMap<>();
        
        File file = this.databaseFile();
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    Dataset ds;
                    ds = new Dataset(tokens[1], tokens[0], tokens[2], tokens[3]);
                    if (query.matchesAtOrBefore(ds)) {
                        if (!db.containsKey(ds.domain())) {
                            db.put(ds.domain(), new HashMap<>());
                        }
                        db.get(ds.domain()).put(ds.identifier(), ds);
                    }
                }
            }
        }
        
        List<Dataset> result = new ArrayList<>();
        for (String domain : db.keySet()) {
            for (Dataset ds : db.get(domain).values()) {
                result.add(ds);
            }
        }
        return result;
    }

    public List<Dataset> getDatasets(DatasetQuery query) throws java.io.IOException {
        
        List<Dataset> db = new ArrayList<>();
        
        File file = this.databaseFile();
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    Dataset ds;
                    ds = new Dataset(tokens[1], tokens[0], tokens[2], tokens[3]);
                    if (query.matches(ds)) {
                        db.add(ds);
                    }
                }
            }
        }
        
        return db;
    }
    
    public List<Dataset> getDatasets() throws java.io.IOException {
        
        return this.getDatasets(new DatasetQuery());
    }
    
    /**
     * Read database file. Contains the last download for each dataset.
     * 
     * The file is expected to be in sequential order such that each file
     * entries overrides previous entries for the same dataset.
     * 
     * @return
     * @throws java.io.IOException 
     */
    public HashMap<String, HashMap<String, Dataset>> getIndex() throws java.io.IOException {
        
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
     * Get the last date on which a dataset was downloaded.
     * 
     * @return 
     */
    public String lastDownloadDate() {
    
        String maxDate = null;
        
        File file = this.databaseFile();
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String date = tokens[2];
                    if (maxDate == null) {
                        maxDate = date;
                    } else if (maxDate.compareTo(date) < 0) {
                        maxDate = date;
                    }
                }
            } catch (java.io.IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        
        return maxDate;
    }
    
    /**
     * Log files are stored in a separate logs directory. Files are named after
     * the download date.
     * 
     * @param date
     * @return 
     */
    public File logFile(String date) {
        
        return FileSystem.joinPath(
                _baseDir,
                new String[]{
                    "logs",
                    date + ".log"
                }
        );
    }
    
    public CSVParser open(Dataset dataset) throws java.io.IOException {
        
        InputStream is = FileSystem.openFile(this.datasetFile(dataset));
        return new CSVParser(
                new InputStreamReader(is),
                CSVFormat.TDF
                        .withFirstRecordAsHeader()
                        .withIgnoreHeaderCase()
                        .withIgnoreSurroundingSpaces(false)
        );
    }
    
    /**
     * Get writer for the database index file.
     * 
     * @param append
     * @return
     * @throws java.io.IOException 
     */
    public DatabaseWriter writer(boolean append) throws java.io.IOException {
        
        return new DatabaseWriter(this.databaseFile(), append);
    }
    
    /**
     * Get database index file writer. By default the writer will append to an
     * existing file.
     * 
     * @return
     * @throws java.io.IOException 
     */
    public DatabaseWriter writer() throws java.io.IOException {
        
        return this.writer(true);
    }
}
