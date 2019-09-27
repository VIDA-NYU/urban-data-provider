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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.query.json.JQuery;

/**
 * Download all datasets from the Socrata API that have been modified since the
 * last download. Downloads datasets in TSV format.
 * 
 * Maintains the list of downloaded files and their last download date in a
 * file on disk that is updated continuously.
 * 
 * All files are downloaded into sub-folders under a base directory. The folders
 * are named by the dataset domain name.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class UpdatedDatasetDownloader {
    
    private static final String COMMAND =  "Usage: <output-directory";
    private static final String DBFILE_NAME = "db.tsv";
    private static final SimpleDateFormat DF = new SimpleDateFormat("yyyyMMdd");
    private static final Logger LOGGER = Logger.getGlobal();
    
    private void download(File outputFile, String url) throws java.io.IOException {
        
        FileSystem.createParentFolder(outputFile);
        
        InputStream in = null;
        OutputStream out = null;
        
        try {
            in = new URL(url).openStream();
            out = FileSystem.openOutputFile(outputFile);
            int c;
            while ((c = in.read()) != -1) {
                out.write(c);
            }
        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
        }
    }

    /**
     * Read database file. the file is expected to be in sequential order such
     * that each file entries overrides previous entries for the same dataset.
     * 
     * @param file
     * @return
     * @throws java.text.ParseException
     * @throws java.io.IOException 
     */
    private HashMap<String, HashMap<String, Date>> readDatabase(File file) throws java.io.IOException {
        
        HashMap<String, HashMap<String, Date>> db = new HashMap<>();
        
        if (file.exists()) {
            try (BufferedReader in = FileSystem.openReader(file)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String domain = tokens[0];
                    String dataset = tokens[1];
                    try {
                        Date date = DF.parse(tokens[2]);
                        if (!db.containsKey(domain)) {
                            db.put(domain, new HashMap<>());
                        }
                        db.get(domain).put(dataset, date);
                    } catch (java.text.ParseException ex) {
                        LOGGER.log(Level.WARNING, tokens[2], ex);
                   }
                }
            }
        }
        
        return db;
    }
    
    public void run(File outputDir) throws java.io.IOException {
        
        // Get the current date and the date key
        Date today = new Date();
        String dateKey = DF.format(today);
        
        // Read the database file containing information about previously
        // downloaded files
        File dbFile = FileSystem.joinPath(outputDir, DBFILE_NAME);
        HashMap<String, HashMap<String, Date>> db = this.readDatabase(dbFile);
        
        // Download the current Socrata catalog
        String catalogName = "catalog." + dateKey + ".json.gz";
        File catalogFile = FileSystem.joinPath(outputDir, catalogName);
        if (!catalogFile.exists()) {
            new SocrataCatalog(catalogFile).download("dataset");
        }
        
        // Query the catalog to get all datasets and their last modification
        // date. Compiles a list of tuples for datasets that need to be
        // downloaded.
        List<JQuery> select = new ArrayList<>();
        select.add(new JQuery("/metadata/domain"));
        select.add(new JQuery("/resource/id"));
        select.add(new JQuery("/resource/data_updated_at"));
        select.add(new JQuery("/permalink"));
        List<String[]> downloads = new ArrayList<>();
        for (String[] tuple : new CatalogQuery(catalogFile).eval(select, true)) {
            String domain = tuple[0];
            String dataset = tuple[1];
            Date lastDownload = null;
            if (db.containsKey(domain)) {
                if (db.get(domain).containsKey(dataset)) {
                    lastDownload = db.get(domain).get(dataset);
                }
            }
            Date lastUpdate;
            try {
                String dt = tuple[2]
                        .substring(0, tuple[2].indexOf("T"))
                        .replaceAll("-", "");
                lastUpdate = DF.parse(dt);
            } catch (java.text.ParseException ex) {
                LOGGER.log(Level.WARNING, tuple[2], ex);
                continue;
            }
            if (lastDownload == null) {
                downloads.add(tuple);
            } else if (lastUpdate.after(lastDownload)) {
                downloads.add(tuple);
            }
        }
        
        LOGGER.log(Level.INFO, "START {0}", new Date());
        
        // Download all updated datasets
        try (PrintWriter out = FileSystem.openPrintWriter(dbFile, true)) {
            for (String[] tuple : downloads) {
                String domain = tuple[0];
                String dataset = tuple[1];
                String permalink = tuple[3];
                if (permalink.contains("/d/")) {
                    String url = permalink.replace("/d/", "/api/views/");
                    url += "/rows.tsv?accessType=DOWNLOAD";
                    LOGGER.log(Level.INFO, url);
                    try {
                        File dir = FileSystem.joinPath(outputDir, domain);
                        dir = FileSystem.joinPath(dir, dateKey);
                        File outputFile = FileSystem.joinPath(dir, dataset + ".tsv.gz");
                        this.download(outputFile, url);
                        out.println(domain + "\t" + dataset + "\t" + dateKey + "\tS");
                    } catch (java.io.IOException ex) {
                        Logger.getGlobal().log(Level.SEVERE, url, ex);
                        out.println(domain + "\t" + dataset + "\t" + dateKey + "\tS");
                    }
                } else {
                    LOGGER.log(Level.WARNING, permalink);
                }
            }
        }

        LOGGER.log(Level.INFO, "DONE {0}", new Date());
    }
    
    public static void main(String[] args) {
    
        if (args.length != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File outputDir = new File(args[0]);
        
        try {
            new UpdatedDatasetDownloader().run(outputDir);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
