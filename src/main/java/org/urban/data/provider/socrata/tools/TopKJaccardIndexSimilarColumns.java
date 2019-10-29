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

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.object.NamedDecimal;
import org.urban.data.core.set.StringSet;
import org.urban.data.core.similarity.JaccardIndex;
import org.urban.data.core.util.FormatedBigDecimal;

/**
 * Compute Top k columns by Jaccard similarity.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TopKJaccardIndexSimilarColumns {
    
    public StringSet readColumn(File file) throws java.io.IOException {
        
        StringSet column = new StringSet();
        
        try (BufferedReader in = FileSystem.openReader(file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String term = line.split("\t")[0];
                column.add(term);
            }
        }
        
        return column;
    }
    
    public void run(
            File inputFile,
            File inputDir,
            int k
    ) throws java.io.IOException {
        
        List<NamedDecimal> ranking = new ArrayList<>();
        
        System.out.println("\nSTART");
        
        StringSet column = this.readColumn(inputFile);
        
        for (File file : inputDir.listFiles()) {
            if (!file.getAbsolutePath().equals(inputFile.getAbsolutePath())) {
                StringSet candidate = this.readColumn(file);
                int ovp = candidate.overlap(column);
                if (ovp > 0) {
                    BigDecimal ji = JaccardIndex
                            .ji(candidate.length(), column.length(), ovp);
                    ranking.add(new NamedDecimal(file.getName(), ji));
                    System.out.println(file.getName() + "\t" + new FormatedBigDecimal(ji));
                }
            }
        }

        Collections.sort(ranking);
        Collections.reverse(ranking);
        
        System.out.println("\n\nRANKING");
        for (int iRank = 0; iRank < Math.min(k, ranking.size()); iRank++) {
            NamedDecimal entry = ranking.get(iRank);
            System.out.println(
                    (iRank + 1) + "\t" +
                    entry.name() + "\t" +
                    new FormatedBigDecimal(entry.value())
            );
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <columns-dir>\n" +
            "  <query-file>\n" +
            "  <k>";
    
    private static final Logger LOGGER = Logger
            .getLogger(TopKJaccardIndexSimilarColumns.class.getName());

    public static void main(String[] args) {
    
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputDir = new File(args[0]);
        File inputFile = new File(args[1]);
        int k = Integer.parseInt(args[2]);
        
        try {
            new TopKJaccardIndexSimilarColumns().run(inputDir, inputFile, k);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
