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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.constraint.Threshold;
import org.urban.data.core.io.FileListReader;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.util.FormatedBigDecimal;

/**
 * Compute Jaccard Index and Jensen-Shannon Divergence between a given set of
 * query columns and a given set of database columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimilarColumnsQuery {
    
    private final Threshold _nGramThreshold;
    
    public SimilarColumnsQuery(Threshold threshold) {
        
        _nGramThreshold = threshold;
    }
    
    public void run(
            List<File> queryFiles,
            List<File> databaseFiles,
            PrintWriter out
    ) throws java.io.IOException {
        
        System.out.println("READ " + queryFiles.size() + " FILES");
        
        List<ColumnValues> query = new ArrayList<>();
        for (File file : queryFiles) {
            query.add(new ColumnValues(file, _nGramThreshold));
        }
        
        System.out.println("\nSTART");
        
        for (File file : databaseFiles) {
            ColumnValues dbCol = new ColumnValues(file, _nGramThreshold);
            for (int iColumn = 0; iColumn < query.size(); iColumn++) {
                ColumnValues qCol = query.get(iColumn);
                if (!qCol.equals(dbCol)) {
                    FormatedBigDecimal ji = new FormatedBigDecimal(qCol.ji(dbCol));
                    FormatedBigDecimal jsd = new FormatedBigDecimal(qCol.jsd(dbCol));
                    String line = qCol.name() + "\t" + dbCol.name() + "\t" + ji + "\t" + jsd;
                    out.println(line);
                    System.out.println(line);
                }
            }
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <query-file(s)>\n" +
            "  <database-file(s)>\n" +
            "  <threshold>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SimilarColumnsQuery.class.getName());

    public static void main(String[] args) {
    
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File queryFileOrDir = new File(args[0]);
        File databaseFileOrDir = new File(args[1]);
        Threshold threshold = Threshold.getConstraint(args[2]);
        File outputFile = new File(args[3]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new SimilarColumnsQuery(threshold).run(
                    new FileListReader(".txt").listFiles(queryFileOrDir),
                    new FileListReader(".txt").listFiles(databaseFileOrDir),
                    out
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
