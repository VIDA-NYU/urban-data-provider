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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.query.json.JsonQuery;
import org.urban.data.core.query.json.ResultTuple;
import org.urban.data.core.query.json.SelectClause;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;
import org.urban.data.provider.socrata.db.DatasetQuery;

/**
 * Get a snapshot of the downloaded datasets for a given date. This will first
 * read the catalog for the date. It will then report for each entry in the
 * catalog the latest downloaded file (if it exists) or indicate that the
 * file does not exist.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatabaseSnapshot implements Command {

    @Override
    public void help(boolean includeDescription) {

        Help.printName(this.name(), "Database snapshot");
        Help.printDir();
        Help.printDomain();
        Help.printDate("Download date (default: last)");
        Help.printDataset();
    }

    @Override
    public String name() {

        return "snapshot";
    }

    @Override
    public void run(Args args) throws IOException {

        DB db = args.getDB();
        
        String date = args.getDateDefaultLast();

        // Query the catalog to get all datasets.
        JsonQuery stmt = new JsonQuery(db.catalogFile(date));
        SelectClause select = new SelectClause()
                .add("domain", new JQuery("/metadata/domain"))
                .add("dataset", new JQuery("/resource/id"))
                .add("name", new JQuery("/resource/name"))
                .add("link", new JQuery("/permalink"));
        List<ResultTuple> rs =  stmt.executeQuery(select, true);

        DatasetQuery query = args.asQuery();
        if (!args.hasDate()) {
            query = query.date(date);
        }
        HashMap<String, HashMap<String, Dataset>> datasets;
        datasets = this.toIndex(db.getSnapshot(query));
        
        int datasetCount = 0;
        int missingDatasets = 0;
        
        for (ResultTuple t : rs) {
            String domainKey = t.get("domain");
            String dsId = t.get("dataset");
            if (!query.matchesAtOrBefore(new Dataset(dsId, domainKey, date))) {
                continue;
            }
            Dataset ds = null;
            if (datasets.containsKey(domainKey)) {
                HashMap<String, Dataset> domain = datasets.get(domainKey);
                if (domain.containsKey(dsId)) {
                    ds = domain.get(dsId);
                }
            }
            datasetCount++;
            System.out.print(domainKey + "\t" + dsId + "\t" + t.get("name"));
            if (ds != null) {
                System.out.println("\ttrue\t" + db.datasetFile(ds).getAbsolutePath());
            } else {
                System.out.println("\tfalse\t" + t.get("link"));
                missingDatasets++;
            }
        }
        System.out.println();
        System.out.println("Catalog entries for " + query + ": " + datasetCount);
        System.out.println("Missing files                : " + missingDatasets);
    }
    
    private HashMap<String, HashMap<String, Dataset>> toIndex(List<Dataset> datasets) {
        
        HashMap<String, HashMap<String, Dataset>> result = new HashMap<>();
        for (Dataset ds : datasets) {
            if (!result.containsKey(ds.domain())) {
                result.put(ds.domain(), new HashMap<>());
            }
            result.get(ds.domain()).put(ds.identifier(), ds);
        }
        return result;
    }
}
