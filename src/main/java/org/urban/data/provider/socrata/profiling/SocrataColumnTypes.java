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
package org.urban.data.provider.socrata.profiling;

import com.google.gson.JsonArray;
import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.query.json.JsonQuery;
import org.urban.data.core.query.json.ResultTuple;
import org.urban.data.core.query.json.SelectClause;

/**
 * Generate a load file containing Socrata data types for all columns in the
 * datasets that are listed in a Socrata catalog file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SocrataColumnTypes {
    
    private static final String COMMAND = "Usage: <catalog-file> <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SocrataColumnTypes.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File catalogFile = new File(args[0]);
        File outputFile = new File(args[1]);

        SelectClause select = new SelectClause();
        select.add("id", new JQuery("/resource/id"));
        select.add("types", new JQuery("/resource/columns_datatype"));
        select.add("names", new JQuery("/resource/columns_name"));
        
        List<ResultTuple> rs = null;
        try {
            rs = new JsonQuery(catalogFile).executeQuery(select);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "QUERY", ex);
            System.exit(-1);
        }
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            CSVPrinter csv = new CSVPrinter(out, CSVFormat.TDF);
            for (ResultTuple row : rs) {
                String datasetId = row.getAsString("id");
                JsonArray types = row.get("types").getAsJsonArray();
                JsonArray names = row.get("names").getAsJsonArray();
                if (types.size() == names.size()) {
                    for (int iColumn = 0; iColumn < types.size(); iColumn++) {
                        String name = names.get(iColumn).getAsString();
                        name = name.replaceAll("\n", "\\n");
                        name = name.replaceAll("\t", " ");
                        String datatype = types.get(iColumn).getAsString();
                        csv.printRecord(datasetId, name, datatype, iColumn);
                    }
                } else {
                    LOGGER.log(Level.SEVERE, "Different array size for {0}", datasetId);
                }
            }
            csv.flush();
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
