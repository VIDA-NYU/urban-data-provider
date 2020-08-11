/*
 * Copyright 2018 New York University.
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

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.urban.data.core.query.JQuery;
import org.urban.data.core.util.FileSystem;

/**
 * Download all dataset files for a given domain from the Socrata API.
 * 
 * Downloads files in JSON format into a given output directory. The directory
 * is created if it does not exist. The names of downloaded files are the
 * Socrata dataset identifier plus suffix .json.
 * 
 * Download is multi-threaded to allow for parallel download of multiple files.
 * 
 * If the overwrite flag is true existing files will be overwritten, otherwise
 * they will be ignored and not downloaded. This flag is primarily intended for
 * cases where a download has to be (partially) repeated (i.e., due to previous
 * errors).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SocrataDatasets {
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <catalog-file>\n" +
            "  <domain>\n" +
            "  <threads>\n" +
            "  <overwrite>\n" +
            "  <dataset-file>\n" +
            "  <output-directory>";
    
    private static final Logger LOGGER = Logger.getGlobal();
    
    public static final String VERSION = "0.1.1";
    
    private class DatasetDownloadTask implements Runnable {
        
        private final ConcurrentLinkedQueue<SocrataDataset> _datasets;
        private final int _id;
        private final File _outputDir;

        /**
         * Initialize the download thread. Each thread has a unique identifier. The
         * identifier is used to decide which elements in the dataset list are
         * downloaded by the thread.
         * 
         * @param id
         * @param datasets
         * @param outputDir 
         */
        public DatasetDownloadTask(
                int id,
                ConcurrentLinkedQueue<SocrataDataset> datasets,
                File outputDir
        ) {
            _id = id;
            _datasets = datasets;
            _outputDir = outputDir;
        }

        @Override
        public void run() {

            int count = 0;
            SocrataDataset dataset;
            while ((dataset = _datasets.poll()) != null) {
                try {
                    dataset.download(_outputDir);
                    System.out.println(_id + ": " + dataset.identifier() + " (" + (++count) + ")");
                } catch (java.net.URISyntaxException | java.io.IOException ex) {
                    LOGGER.log(Level.SEVERE, null, ex);
                }
            }
        }
    }
    
    public void run(
            File catalogFile,
            String domain,
            boolean overwrite,
            int threads,
            File datasetFile,
            File outputDir
    ) throws java.io.IOException, java.lang.InterruptedException, java.net.URISyntaxException {
        
        // Create output directory if it does not exist
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        // Create a list of existing files (only if overwrite is False). Exiting
        // files will be ignored during download in this case. It is assumed
        // that the file name equals the dataset identifier plus a .json or
        // .json.gz suffix.
	HashSet<String> existingFiles = new HashSet<>();
	if (!overwrite) {
	    for (File file : outputDir.listFiles()) {
		if (file.getName().endsWith(".json")) {
		    String[] tokens = file.getName().split("\\.");
		    if (tokens.length == 2) {
			existingFiles.add(tokens[0]);
		    }
		} else if (file.getName().endsWith(".json.gz")) {
		    String[] tokens = file.getName().split("\\.");
		    if (tokens.length == 3) {
			existingFiles.add(tokens[0]);
		    }
		}
	    }
	}
        
        // Download catalog if file does not exists
        if (!catalogFile.exists()) {
            new SocrataCatalog(catalogFile).download(domain, "dataset");
        }
        
        // Query catalog to get dataset identifier and permalink information
        ArrayList<JQuery> select = new ArrayList<>();
        select.add(new JQuery("resource/id"));
        select.add(new JQuery("resource/name"));
        select.add(new JQuery("permalink"));
        select.add(new JQuery("metadata/domain"));

        HashMap<String, String[]> tuples = new HashMap<>();
        for (String[] tuple : new SocrataCatalog(catalogFile).query(select)) {
            if (tuple[3].equals(domain)) {
                tuples.put(tuple[0], tuple);
            }
        }
        
        System.out.println("GET DOWNLOAD URL FOR " + tuples.size() + " DATASET RESOURCES");
        
        // Fetch download Url's for datasets and write dataset infor file.
        ConcurrentLinkedQueue<SocrataDataset> datasets;
        datasets = new ConcurrentLinkedQueue<>();
        try (PrintWriter out = FileSystem.openPrintWriter(datasetFile)) {
            for (String id : tuples.keySet()) {
                if (!existingFiles.contains(id)) {
                    String name = tuples.get(id)[1];
                    String permalink = tuples.get(id)[2];
                    try {
                        datasets.add(new SocrataDataset(permalink));
                        out.println(id + "\t" + name);
                    } catch (java.lang.IllegalArgumentException ex) {
                        LOGGER.log(Level.INFO, "No resource Url for {0} ({1})", new Object[]{id, permalink});
                    } catch (java.io.IOException ex) {
                        LOGGER.log(Level.INFO, "Dataset " + id + " " + permalink, ex);
                    }
                }
            }
        }
        
        System.out.println("DOWNLOAD " + datasets.size() + " DATASETS");
        
        // Start parallel downloading of all dataset files.
	threads = Math.max(1, Math.min(datasets.size(), threads));
        ExecutorService es = Executors.newCachedThreadPool();
        for (int iThread = 0; iThread < threads; iThread++) {
            es.execute(new DatasetDownloadTask(iThread, datasets, outputDir));
        }
        es.shutdown();
        es.awaitTermination(1, TimeUnit.DAYS);
    }
    
    public static void main(String[] args) {
        
	System.out.println("Urban Data Integration - Socrata Dataset Download - Version (" + VERSION + ")\n");

        if (args.length != 6) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File catalogFile = new File(args[0]);
        String domain = args[1];
        int threads = Integer.parseInt(args[2]);
        boolean overwrite = Boolean.parseBoolean(args[3]);
        File datasetFile = new File(args[4]);
        File outputDir = new File(args[5]);
        
        try {
            new SocrataDatasets()
                    .run(
                            catalogFile,
                            domain,
                            overwrite,
                            threads,
                            datasetFile,
                            outputDir
                    );
        } catch (java.lang.InterruptedException | java.io.IOException | java.net.URISyntaxException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
