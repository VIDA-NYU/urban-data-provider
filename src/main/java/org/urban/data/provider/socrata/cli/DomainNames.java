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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.urban.data.core.query.JQuery;
import org.urban.data.core.query.JsonQuery;
import org.urban.data.core.query.ResultTuple;
import org.urban.data.core.query.SelectClause;
import org.urban.data.core.util.Counter;
import org.urban.data.provider.socrata.db.DB;

/**
 * Query the catalog to get the names of all domains. Outputs the domain
 * names sorted in alphabetical order together with the number of datasets.
 * Also prints a summary with the total number of domains and datasets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainNames extends CommandImpl implements Command {
   
    public DomainNames() {

        super("domains", "Query catalog file for domain names");
        this.addParameter(Args.PARA_DATE, "Date for catalog file (default: today)");
    }

    @Override
    public void run(Args args) throws java.io.IOException {
        
        DB db = args.getDB();
        String date = args.getDateDefaultLast();
        
        SelectClause select = new SelectClause()
                .add("domain", new JQuery("/metadata/domain"));
        
        HashMap<String, Counter> domains = new HashMap<>();
        
        JsonQuery con = new JsonQuery(db.catalogFile(date));
        for (ResultTuple tuple : con.executeQuery(select, true)) {
        	String domain = tuple.getAsString("domain");
        	if (domains.containsKey(domain)) {
        		domains.get(domain).inc();
        	} else {
        		domains.put(domain, new Counter(1));
        	}
        }
        
        List<String> domainNames = new ArrayList<>(domains.keySet());
        Collections.sort(domainNames);
        
        PrintWriter out = new PrintWriter(System.out);
        out.println("Domains for catalog from " + date);
        
        int datasetCount = 0;
        for (String domain : domainNames) {
        	int datasets = domains.get(domain).value();
        	out.println(String.format("%s\t%d", domain, datasets));
        	datasetCount += datasets;
        }
        
        out.println("\nSummary for catalog from " + date);
        out.println(String.format("Total number of domains : %d", domainNames.size()));
        out.println(String.format("Total number of datasets: %d", datasetCount));
        out.flush();
    }
}
