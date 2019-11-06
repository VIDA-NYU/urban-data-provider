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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.constraint.Threshold;
import org.urban.data.core.io.FileListReader;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.util.StringHelper;

/**
 * Compute Top k similar columns.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TopKSimilarColumnsPrinter {
    
    public void run(
            File queryFile,
            List<File> database,
            Threshold threshold,
            int k,
            PrintWriter out
    ) throws java.io.IOException {
        
        System.out.println("DATABASE HAS " + database.size() + " COLUMNS");
        
        HashMap<String, List<File>> queries = new HashMap<>();
        
        try (BufferedReader in = FileSystem.openReader(queryFile)) {
            boolean done = false;
            while (!done) {
                String headline = in.readLine();
                if (headline == null) {
                    done = true;
                } else {
                    in.readLine();
                    List<File> queryFiles = new ArrayList<>();
                    String line;
                    while ((line = in.readLine()) != null) {
                        if (line.trim().equals("")) {
                            break;
                        } else {
                            queryFiles.add(new File(line));
                        }
                    }
                    queries.put(headline, queryFiles);
                }
            }
        }
        
        for (String headline : queries.keySet()) {
            out.println(headline);
            out.println(StringHelper.repeat("=", headline.length()));
            System.out.println(headline);
            System.out.println(StringHelper.repeat("=", headline.length()));
            List<File> query = queries.get(headline);
            for (File file : query) {
                System.out.println(file.getName());
                out.println(file.getName());
            }
            System.out.println();
            out.println();
            for (File file : new SimilarColumnsQuery(threshold).eval(query, database, k)) {
                System.out.println(file.getName());
                out.println(file.getName());
            }
            System.out.println();
            out.println();
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
            new TopKSimilarColumnsPrinter().run(
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
