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
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.util.count.Counter;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CopyBigDataColumnFiles {
    
    private static void copy(File inputFile, File outputFile) throws java.io.IOException {
    
        HashMap<String, Counter> termIndex = new HashMap<>();
        
        try (BufferedReader in = FileSystem.openReader(inputFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                String term = tokens[0].trim();
                int count = Integer.parseInt(tokens[1]);
                if (!termIndex.containsKey(term)) {
                    termIndex.put(term, new Counter(count));
                } else {
                    termIndex.get(term).inc(count);
                }
            }
        }
        
        ArrayList<String> terms = new ArrayList<>(termIndex.keySet());
        Collections.sort(terms);
        
        try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
            for (String term : terms) {
                out.println(term + "\t" + termIndex.get(term).value());
            }
        }
    }
    
    private final static String COMMAND = "Usage: <cp.txt> <input-dir> <output-dir>";
    
    public static void main(String[] args) {
    
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File copyFile = new File(args[0]);
        File inputDir = new File(args[1]);
        File outputDir = new File(args[2]);
        
        try (BufferedReader in = FileSystem.openReader(copyFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                File inFile = FileSystem.joinPath(inputDir, tokens[0]);
                File outFile = FileSystem.joinPath(outputDir, tokens[1]);
                CopyBigDataColumnFiles.copy(inFile, outFile);
            }
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
