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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.constraint.Threshold;
import org.urban.data.core.io.FileListReader;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.util.FormatedBigDecimal;

/**
 * Compute Top k similar columns based on ngrams and Jensen-Shannon Similarity.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TopKJSDColumnsPrinter {
    
    public void run(
            File queryFile,
            List<File> database,
            Threshold threshold,
            int k,
            PrintWriter out
    ) throws java.io.IOException {
        
        System.out.println("DATABASE HAS " + database.size() + " COLUMNS");
        
        ColumnValues query = new ColumnValues(queryFile, threshold);

        List<ColumnFileScore> scores = new ArrayList<>();
        for (File file : database) {
            if (!queryFile.getAbsolutePath().equals(file.getAbsolutePath())) {
                ColumnValues dbCol = new ColumnValues(file, threshold);
                BigDecimal jsd = BigDecimal.ONE.subtract(query.jsd(dbCol));
                if (jsd.compareTo(BigDecimal.ZERO) > 0) {
                    scores.add(new ColumnFileScore(file, jsd));
                }
            }
        }
        
        Collections.sort(scores);
        Collections.reverse(scores);
        
        for (int iRank = 0; iRank < Math.min(scores.size(), k); iRank++) {
            ColumnFileScore score = scores.get(iRank);
            System.out.println(score.file().getName() + "\t" + new FormatedBigDecimal(score.value()));
            out.println(score.file().getName() + "\t" + new FormatedBigDecimal(score.value()));
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <query-file>\n" +
            "  <database-file>\n" +
            "  <threshold>\n" +
            "  <k>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SimilarColumnsQuery.class.getName());

    public static void main(String[] args) {
    
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File queryFile = new File(args[0]);
        File databaseFile = new File(args[1]);
        Threshold threshold = Threshold.getConstraint(args[2]);
        int k = Integer.parseInt(args[3]);
        File outputFile = new File(args[4]);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            new TopKJSDColumnsPrinter().run(
                    queryFile,
                    new FileListReader(".txt").listFiles(databaseFile),
                    threshold,
                    k,
                    out
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
