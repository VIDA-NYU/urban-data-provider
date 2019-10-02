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

import org.urban.data.provider.socrata.cli.Args;
import java.io.PrintWriter;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.query.json.JsonQuery;
import org.urban.data.core.query.json.ResultTuple;
import org.urban.data.core.query.json.SelectClause;
import org.urban.data.provider.socrata.cli.Command;
import org.urban.data.provider.socrata.cli.Help;

/**
 * Query the catalog to get the names of all datasets. Allows to restrict the
 * result set to datasets fro a given domain.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatasetNameQuery implements Command {
   
    @Override
    public void help() {

        Help.printName(this.name(), "Query catalog file for dataset names");
        Help.printDir();
        Help.printDomain();
        Help.printDataset();
        Help.printDate("Date for catalog file (default: today)");
        Help.printOutput("Output file (default: standard output)");
    }

    @Override
    public String name() {

        return "query";
    }

    @Override
    public void run(Args args) throws java.io.IOException {
        
        DB db = args.getDB();
        
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
        
        JsonQuery con = new JsonQuery(db.catalogFile(args.getDate()));
        for (ResultTuple tuple : con.executeQuery(select, true)) {
            boolean output = true;
            if (args.hasDomain()) {
                output = tuple.get("domain").equals(args.getDomain());
            }
            if ((output) && (args.hasDataset())) {
                output = tuple.get("dataset").equals(args.getDataset());
            }
            if (output) {
                out.println(tuple.join("\t"));
            }
        }
        
        out.close();
    }
}
