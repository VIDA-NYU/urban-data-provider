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
package org.urban.data.provider.socrata.cli;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.apache.commons.io.IOUtils;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.query.json.JsonQuery;
import org.urban.data.core.query.json.ResultTuple;
import org.urban.data.core.query.json.SelectClause;
import org.urban.data.provider.socrata.SocrataCatalog;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.DatabaseWriter;
import org.urban.data.provider.socrata.db.Dataset;
import org.urban.data.provider.socrata.db.DatasetQuery;

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
public class DownloadDatasets extends CommandImpl implements Command {
    
    private static final Logger LOGGER = Logger
            .getLogger(DownloadDatasets.class.getName());

    private class DownloadTask implements Runnable {

        private final ConcurrentLinkedQueue<ResultTuple> _datasets;
        private final DB _db;
        private final String _date;
        private final DatabaseWriter _writer;
        
        public DownloadTask(
                ConcurrentLinkedQueue<ResultTuple> datasets,
                DB db,
                String date,
                DatabaseWriter writer
        ) {
        
            _datasets = datasets;
            _db = db;
            _date = date;
            _writer = writer;
        }
        
        private void download(File outputFile, String url) throws java.io.IOException {

            FileSystem.createParentFolder(outputFile);
            
            if (!outputFile.exists()) {
                try (
                        InputStream in = new URL(url).openStream();
                        OutputStream out = FileSystem.openOutputFile(outputFile)
                ) {
                    IOUtils.copy(in, out);
                }
            }            
        }

        @Override
        public void run() {

            ResultTuple tuple;
            while ((tuple = _datasets.poll()) != null) {
                String domain = tuple.get("domain");
                String dataset = tuple.get("dataset");
                String permalink = tuple.get("link");
                if (permalink.contains("/d/")) {
                    String url = permalink.replace("/d/", "/api/views/");
                    url += "/rows.tsv?accessType=DOWNLOAD";
                    LOGGER.log(Level.INFO, url);
                    String state;
                    try {
                        File file = _db.datasetFile(dataset, domain, _date);
                        this.download(file, url);
                        state = DB.DOWNLOAD_SUCCESS;
                    } catch (java.io.IOException ex) {
                        LOGGER.log(Level.SEVERE, url, ex);
                        state = DB.DOWNLOAD_FAILED;
                    }
                    _writer.write(new Dataset(dataset, domain, _date, state));
                } else {
                    LOGGER.log(Level.WARNING, permalink);
                }
            }
        }
        
    }

    public DownloadDatasets() {

        super("download", "Download datasets that have changed");
        this.addParameter(Args.PARA_DOMAIN);
        this.addParameter(Args.PARA_DATASET);
        this.addParameter(Args.PARA_DATE, "Date for catalog file (default: today)");
    }

    @Override
    public void run(Args args) throws IOException {
        
        DB db = args.getDB();
        String date = args.getDateDefaultToday();
        int threads = args.getThreads();
        
        // Configure the log file
        File logFile = db.logFile(date);
        FileSystem.createParentFolder(logFile);
        FileHandler fh = new FileHandler(logFile.getAbsolutePath());
        fh.setFormatter(new SimpleFormatter());
        LOGGER.addHandler(fh);
        LOGGER.setLevel(Level.INFO);
        
        // Read the database file containing information about previously
        // downloaded files
        HashMap<String, HashMap<String, Dataset>> datasets = db.getIndex();
        
        // Download the current Socrata catalog
        File catalogFile = db.catalogFile(date);
        if (!catalogFile.exists()) {
            FileSystem.createParentFolder(catalogFile);
            new SocrataCatalog(catalogFile).download("dataset");
        }
        
        // Query the catalog to get all datasets and their last modification
        // date. Compiles a list of tuples for datasets that need to be
        // downloaded.
        SelectClause select = new SelectClause()
                .add("domain", new JQuery("/metadata/domain"))
                .add("dataset", new JQuery("/resource/id"))
                .add("updatedAt", new JQuery("/resource/data_updated_at"))
                .add("link", new JQuery("/permalink"));
        
        ConcurrentLinkedQueue<ResultTuple> downloads = new ConcurrentLinkedQueue<>();
        
        DatasetQuery query = args.asQuery();
        
        List<ResultTuple> rs =  new JsonQuery(catalogFile).executeQuery(select, true);
        for (ResultTuple tuple : rs) {
            String domain = tuple.get("domain");
            String dataset = tuple.get("dataset");
            if (!query.matches(new Dataset(dataset, domain, date))) {
                continue;
            }
            Date lastDownload = null;
            if (datasets.containsKey(domain)) {
                if (datasets.get(domain).containsKey(dataset)) {
                    Dataset ds = datasets.get(domain).get(dataset);
                    if (ds.successfulDownload()) {
                        lastDownload = ds.getDate();
                    }
                }
            }
            Date lastUpdate;
            try {
                String dt = tuple.get("updatedAt")
                        .substring(0, tuple.get("updatedAt").indexOf("T"))
                        .replaceAll("-", "");
                lastUpdate = DB.DF.parse(dt);
            } catch (java.text.ParseException ex) {
                LOGGER.log(Level.WARNING, tuple.get("updatedAt"), ex);
                continue;
            }
            if (lastDownload == null) {
                downloads.add(tuple);
            } else if (lastUpdate.after(lastDownload)) {
                downloads.add(tuple);
            }
        }
        
        LOGGER.log(Level.INFO, "DOWNLOAD {0} FILES", downloads.size());
        LOGGER.log(Level.INFO, "START {0}", new Date());
        
        // Download all updated datasets
        try (DatabaseWriter writer = db.writer()) {
            ExecutorService es = Executors.newCachedThreadPool();
            for (int iThread = 0; iThread < threads; iThread++) {
                es.execute(new DownloadTask(downloads, db, date, writer));
            }
            es.shutdown();
            try {
                es.awaitTermination(7, TimeUnit.DAYS);
            } catch (java.lang.InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }

        LOGGER.log(Level.INFO, "DONE {0}", new Date());
    }
}
