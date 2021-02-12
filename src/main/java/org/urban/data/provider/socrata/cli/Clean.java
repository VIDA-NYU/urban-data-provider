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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.io.FileSystem;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;

/**
 * Remove all downloaded files that are empty, flagged as not successful or
 * that seem to contain a HTML document.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Clean extends CommandImpl implements Command {

    private static final String[] HTML_LABELS = {
        "!DOCTYPE",
        "html",
        "head",
        "script",
        "body",
        "meta",
        "link"
    };
    
    public Clean() {

        super("clean", "Remove empty datasets and HTML files");
        this.addParameter(Args.PARA_DOMAIN);
        this.addParameter(Args.PARA_DATE, "Date of download");
        this.addParameter(Args.PARA_HTML);
        this.addParameter(Args.PARA_REPORT);
    }

    private boolean markForDelete(File file, boolean includeHtml) throws java.io.IOException {

        int lineCount = 0;
        
        boolean multiColumn = false;
        HashSet<String> matchedLabels = new HashSet<>();
        
        try (InputStream is = FileSystem.openFile(file)) {
            CSVParser in = new CSVParser(new InputStreamReader(is), CSVFormat.TDF);
            try {
                for (CSVRecord record : in) {
                    lineCount++;
                    if (record.size() == 1) {
                        String value = record.get(0).trim();
                        if (value.toUpperCase().startsWith("<!DOCTYPE HTML")) {
                            lineCount = 0;
                            break;
                        }
                        for (String label : HTML_LABELS) {
                            if (!matchedLabels.contains(label)) {
                                if (value.startsWith("<" + label)) {
                                    matchedLabels.add(label);
                                    break;
                                }
                            }
                        }
                    } else {
                        multiColumn = true;
                        break;
                    }
                }
            } catch (Exception ex) {
                return true;
            }
        }
        
        if (lineCount == 0) {
            return true;
        } else if ((!multiColumn) && (includeHtml)) {
            return matchedLabels.size() >= 4;
        }
        return false;
    }
    
    @Override
    public void run(Args args) throws java.io.IOException {

        DB db = args.getDB();
        
        List<Dataset> datasets = db.getDatasets(args.asQuery());
        
        List<Dataset> deleteDatasets = new ArrayList<>();
        for (Dataset dataset : datasets) {
            File file = db.datasetFile(dataset);
            if ((file.exists()) && (dataset.successfulDownload())) {
                if (this.markForDelete(file, args.getHtml())) {
                    deleteDatasets.add(dataset);
                }
            } else {
                deleteDatasets.add(dataset);
            }
        }
        
        for (Dataset dataset : deleteDatasets) {
            System.out.println(
                    dataset.domain() + "\t" +
                    dataset.identifier() + "\t" +
                    dataset.downloadDate()
            );
        }
        System.out.println(deleteDatasets.size() + " datasets");
        
        if (!args.getReport()) {
            db.deleteDatasets(deleteDatasets);
        }    
    }
}
