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

import com.google.gson.JsonArray;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import org.urban.data.core.query.JQuery;
import org.urban.data.core.query.JsonQuery;
import org.urban.data.core.query.ResultTuple;
import org.urban.data.core.query.SelectClause;
import org.urban.data.core.util.FileSystem;
import org.urban.data.provider.socrata.SocrataCatalog;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;
import org.urban.data.provider.socrata.db.DatasetQuery;

/**
 * Export data type information for downloaded datasets from the catalog file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExportColumnTypes extends CommandImpl implements Command {
    
    public ExportColumnTypes() {

        super("export types", "Export data type information for dataset columns");
        this.addParameter(Args.PARA_DOMAIN);
        this.addParameter(Args.PARA_DATASET);
        this.addParameter(Args.PARA_DATE, "Date for catalog file (default: today)");
        this.addParameter(Args.PARA_OUTPUT, "Output file");
    }

    @Override
    public void run(Args args) throws IOException {
        
        DB db = args.getDB();
        String date = args.getDateDefaultToday();
        
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
                .add("names", new JQuery("/resource/columns_field_name"))
                .add("datatypes", new JQuery("/resource/columns_datatype"));
        
        DatasetQuery query = args.asQuery();
        
        File outputFile = args.getOutput();
        FileSystem.createParentFolder(outputFile);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            List<ResultTuple> rs =  new JsonQuery(catalogFile).executeQuery(select, true);
            for (ResultTuple tuple : rs) {
                String domain = tuple.get("domain").getAsString();
                String dataset = tuple.get("dataset").getAsString();
                if (!query.matches(new Dataset(dataset, domain, date))) {
                    continue;
                }
                JsonArray names = tuple.get("names").getAsJsonArray();
                JsonArray datatypes = tuple.get("datatypes").getAsJsonArray();
                for (int iColumn = 0; iColumn < names.size(); iColumn++) {
                    out.println(
                            String.format(
                                    "%s\t%s\t%d\t%s\t%s",
                                    domain,
                                    dataset,
                                    iColumn,
                                    names.get(iColumn).getAsString(),
                                    datatypes.get(iColumn).getAsString().toLowerCase()
                            )
                    );
                }
            }
        }
    }
}
