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

import java.io.PrintWriter;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.query.json.JsonQuery;
import org.urban.data.core.query.json.ResultTuple;
import org.urban.data.core.query.json.SelectClause;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;
import org.urban.data.provider.socrata.db.DatasetQuery;

/**
 * Query the catalog to get the names of all datasets. Allows to restrict the
 * result set to datasets fro a given domain.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatasetNames extends CommandImpl implements Command {
   
    public DatasetNames() {

        super("dataset names", "Query catalog file for dataset names");
        this.addParameter(Args.PARA_DOMAIN);
        this.addParameter(Args.PARA_DATASET);
        this.addParameter(Args.PARA_DATE, "Date for catalog file (default: today)");
        this.addParameter(Args.PARA_OUTPUT, "Output file (default: standard output)");
        this.addParameter(Args.PARA_EXISTING);
    }

    @Override
    public void run(Args args) throws java.io.IOException {
        
        DB db = args.getDB();
        String date = args.getDateDefaultLast();
        boolean existingOnly = args.getExisting();
        
        System.out.println("Dataset names for catalog from " + date);
        
        PrintWriter out;
        if (args.hasOutput()) {
            out = FileSystem.openPrintWriter(args.getOutput());
        } else {
            out = new PrintWriter(System.out);
        }
        
        SelectClause select = new SelectClause()
                .add("domain", new JQuery("/metadata/domain"))
                .add("dataset", new JQuery("/resource/id"))
                .add("name", new JQuery("/resource/name"));
        
        DatasetQuery query = new DatasetQuery()
                .domain(args.getDomain())
                .dataset(args.getDataset());
        
        JsonQuery con = new JsonQuery(db.catalogFile(date));
        for (ResultTuple tuple : con.executeQuery(select, true)) {
            Dataset dataset = new Dataset(
                    tuple.getAsString("dataset"),
                    tuple.getAsString("domain"),
                    date
            );
            if (query.matches(dataset)) {
                if (existingOnly) {
                    if (!db.datasetFile(dataset).exists()) {
                        continue;
                    }
                }
                out.println(tuple.join("\t"));
            }
        }
        
        out.close();
    }
}
