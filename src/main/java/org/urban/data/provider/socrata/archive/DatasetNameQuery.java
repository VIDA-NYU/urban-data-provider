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
import java.io.PrintWriter;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.query.json.JsonQuery;
import org.urban.data.core.query.json.ResultTuple;
import org.urban.data.core.query.json.SelectClause;

/**
 * Query the catalog to get the names of all datasets. Allows to restrict the
 * result set to datasets fro a given domain.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatasetNameQuery {
   
    public void run(DB db, Args args, PrintWriter out) throws java.io.IOException {
        
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
    }

    private final static String COMMAND =
            "Usage:\n" +
            "  {--domain=<domain>}\n" +
            "  {--date=<date>}\n" +
            "  {--dataset=<dataset>}\n" +
            "  <database-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(DatasetNameQuery.class.getName());
    
    public static void main(String[] args) {
        
        if ((args.length == 0) || (args.length > 4)) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        Args cmd = new Args(args, COMMAND);
        if (cmd.size() != 1) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        DB db = new DB(new File(cmd.get(0)));
        if (cmd.hasDate()) {
            try (PrintWriter out = new PrintWriter(System.out)) {
                new DatasetNameQuery().run(db, cmd, out);
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "RUN", ex);
                System.exit(-1);
            }
        } else {
            System.out.println("Not supported yet.");
            System.exit(0);
        }
    }
}
