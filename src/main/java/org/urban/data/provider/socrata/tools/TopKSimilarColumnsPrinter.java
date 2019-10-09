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
package org.urban.data.provider.socrata.tools;

import java.io.File;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.set.StringSet;
import org.urban.data.core.similarity.JaccardIndex;
import org.urban.data.core.util.FormatedBigDecimal;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;
import org.urban.data.provider.socrata.db.DatasetQuery;

/**
 * Compute Top k similar columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TopKSimilarColumnsPrinter {
    
    private static final Logger LOGGER = Logger
            .getLogger(TopKSimilarColumnsPrinter.class.getName());
    

    public StringSet readColumn(DB db, Dataset dataset, String columnName) throws java.io.IOException {
        
        StringSet column = new StringSet();
        
        try (CSVParser in = db.open(dataset)) {
            for (CSVRecord record : in) {
                String term = record.get(columnName);
                if (!column.contains(term)) {
                    column.add(term);
                }
            }
        }
        
        System.out.println("READ " + column.length() + " DISTINCT VALUES");
        
        return column;
    }
    
    public void run(
            DB db,
            DatasetQuery query,
            StringSet column,
            int k
    ) throws java.io.IOException {
        
        TopKList ranking = new TopKList(k);
        
        System.out.println("\nSTART");
        
        for (Dataset dataset : db.getDatasets(query)) {
            HashMap<String, StringSet> columns = new HashMap<>();
            try (CSVParser in = db.open(dataset)) {
                List<String> columnNames = in.getHeaderNames();
                for (String colName : columnNames) {
                    columns.put(colName, new StringSet());
                }
                try {
                    for (CSVRecord record : in) {
                        for (String colName : columnNames) {
                            String term = record.get(colName);
                            StringSet colMatches = columns.get(colName);
                            if (!colMatches.contains(term)) {
                                colMatches.add(term);
                            }
                        }
                    }
                    for (String colName : columnNames) {
                        StringSet candidate = columns.get(colName);
                        int ovp = candidate.overlap(column);
                        if (ovp > 0) {
                            BigDecimal ji = JaccardIndex
                                    .ji(candidate.length(), column.length(), ovp);
                            ranking.add(dataset, colName, ji);
                            System.out.println(dataset + "\t" + colName + "\t" + new FormatedBigDecimal(ji));
                        }
                    }
                } catch (java.lang.Exception ex) {
                    LOGGER.log(Level.SEVERE, dataset.toString(), ex);
                }
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.WARNING, dataset.toString(), ex);
            }
        }

        System.out.println("\n\nRANKING");
        for (int iRank = 0; iRank < ranking.size(); iRank++) {
            TopKListEntry entry = ranking.get(iRank);
            System.out.println(
                    (iRank + 1) + "\t" +
                    entry.dataset() + "\t" +
                    entry.column() + "\t" +
                    new FormatedBigDecimal(entry.sim())
            );
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <database-dir>\n" +
            "  <query>\n" +
            "  <download>\n" +
            "  <k>";
    
    public static void main(String[] args) {
    
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File databaseDir = new File(args[0]);
        String[] columnSpec = args[1].split("/");
        String[] downloadSpec = args[2].split("/");
        int k = Integer.parseInt(args[3]);
        
        DB db = new DB(databaseDir);
        
        DatasetQuery query = new DatasetQuery()
                .domain(downloadSpec[0])
                .date(downloadSpec[1]);
        
        TopKSimilarColumnsPrinter computer = new TopKSimilarColumnsPrinter();
        
        try {
            computer.run(
                    db,
                    query,
                    computer.readColumn(
                            db,
                            new Dataset(columnSpec[2], columnSpec[0], columnSpec[1]),
                            columnSpec[3]
                    ),
                    k
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
