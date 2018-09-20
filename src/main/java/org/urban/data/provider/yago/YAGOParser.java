/*
 * Copyright 2018 New York University.
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
package org.urban.data.provider.yago;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.io.FileSystem;

/**
 * Extract English language terms and their types.
 * 
 * Outputs a tab-delimited file with two columns:
 * 
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class YAGOParser {
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <yago-label-file> (e.g., yagoLabels.tsv)\n" +
            "  <yago-type-file> (e.g., yagoTypes.tsv)\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger.getGlobal();
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File yagoLabelFile = new File(args[0]);
        File yagoTypeFile = new File(args[1]);
        File outputFile = new File(args[2]);

        // Get set of all english terms (i.e., labels ending with @eng) and
        // their rdf:source. The mapping is later used to map labels to
        // ontology types.
        HashMap<String, String> terms = new HashMap<>();
        try (BufferedReader in = FileSystem.openReader(yagoLabelFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                if (tokens.length == 4) {
                    if ((tokens[2].equals("rdfs:label")) || (tokens[2].equals("<redirectedFrom>"))) {
                        String source = tokens[1].substring(1, tokens[1].length() - 1);
                        String value = tokens[3];
                        if (value.endsWith("@eng")) {
                            value = value.substring(1, value.length() - 5).toUpperCase();
                            terms.put(source, value);
                        }
                    }
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            System.exit(-1);
        }
        
        System.out.println("TOTAL NUMBER OF TERMS: " + terms.size());
        
        try (
                BufferedReader in = FileSystem.openReader(yagoTypeFile);
                PrintWriter out = FileSystem.openPrintWriter(outputFile)
        ) {
            // Skip the first line that contains the license statement.
            String line = in.readLine();
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                if (tokens.length == 4) {
                    String source = tokens[1].substring(1, tokens[1].length() - 1);
                    String type = tokens[3].substring(1, tokens[3].length() - 1);
                    if (terms.containsKey(source)) {
                        out.println(terms.get(source) + "\t" + type);
                    }
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
            System.exit(-1);
        }
    }
}
