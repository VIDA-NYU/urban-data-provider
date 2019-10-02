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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
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
import org.urban.data.core.io.SynchronizedWriter;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.query.json.JsonQuery;
import org.urban.data.core.query.json.ResultTuple;
import org.urban.data.core.query.json.SelectClause;
import org.urban.data.provider.socrata.SocrataCatalog;
import org.urban.data.provider.socrata.cli.Args;
import org.urban.data.provider.socrata.cli.Command;
import org.urban.data.provider.socrata.cli.Help;

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
public class UpdatedDatasetDownloader implements Command {
    
    private static final Logger LOGGER = Logger
            .getLogger(UpdatedDatasetDownloader.class.getName());

    private class DownloadTask implements Runnable {

        private final ConcurrentLinkedQueue<ResultTuple> _datasets;
        private final String _dateKey;
        private final File _outputDir;
        private final SynchronizedWriter _writer;
        
        public DownloadTask(
                ConcurrentLinkedQueue<ResultTuple> datasets,
                String dateKey,
                File outputDir,
                SynchronizedWriter writer
        ) {
        
            _datasets = datasets;
            _dateKey = dateKey;
            _outputDir = outputDir;
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
                        File dir = FileSystem.joinPath(_outputDir, domain);
                        dir = FileSystem.joinPath(dir, _dateKey);
                        File outputFile = FileSystem.joinPath(dir, dataset + ".tsv.gz");
                        this.download(outputFile, url);
                        state = DB.DOWNLOAD_SUCCESS;
                    } catch (java.io.IOException ex) {
                        LOGGER.log(Level.SEVERE, url, ex);
                        state = DB.DOWNLOAD_FAILED;
                    }
                    _writer.write(domain + "\t" + dataset + "\t" + _dateKey + "\t" + state);
                    _writer.flush();
                } else {
                    LOGGER.log(Level.WARNING, permalink);
                }
            }
        }
        
    }

    @Override
    public void help() {

        Help.printName(this.name(), "Download datasets that have changed");
        Help.printDir();
        Help.printDate("Date for catalog file (default: today)");
    }

    @Override
    public String name() {

        return "download";
    }

    public void run(File databseDir, String date, int threads) throws java.io.IOException {
        
        // Configure the log file
        File logFile = FileSystem.joinPath(databseDir, "logs");
        logFile = FileSystem.joinPath(logFile, date + ".log");
        FileSystem.createParentFolder(logFile);
        FileHandler fh = new FileHandler(logFile.getAbsolutePath());
        fh.setFormatter(new SimpleFormatter());
        LOGGER.addHandler(fh);
        LOGGER.setLevel(Level.INFO);
        
        // Read the database file containing information about previously
        // downloaded files
        DB db = new DB(databseDir);
        HashMap<String, HashMap<String, Dataset>> datasets = db.readIndex();
        
        // Download the current Socrata catalog
        File catalogFile = db.catalogFile(date);
        if (!catalogFile.exists()) {
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
        
        List<ResultTuple> rs =  new JsonQuery(catalogFile).executeQuery(select, true);
        for (ResultTuple tuple : rs) {
            String domain = tuple.get("domain");
            String dataset = tuple.get("dataset");
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
        try (PrintWriter out = FileSystem.openPrintWriter(db.databaseFile(), true)) {
            SynchronizedWriter writer = new SynchronizedWriter(out);
            ExecutorService es = Executors.newCachedThreadPool();
            for (int iThread = 0; iThread < threads; iThread++) {
                es.execute(new DownloadTask(downloads, date, databseDir, writer));
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
    
    @Override
    public void run(Args args) throws IOException {

        this.run(args.getDirectory(), args.getDate(), args.getThreads());
    }
    
    private static final String COMMAND = 
            "Usage:\n" +
            "  <output-directory>\n" +
            "  <threads>\n" +
            "  {<date>}";

    public static void main(String[] args) {
    
        if ((args.length < 2) || (args.length > 3)) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File outputDir = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        String dateKey;
        if (args.length == 3) {
            dateKey = args[2];
        } else {
            dateKey = DB.DF.format(new Date());
        }
        
        try {
            new UpdatedDatasetDownloader().run(outputDir, dateKey, threads);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
