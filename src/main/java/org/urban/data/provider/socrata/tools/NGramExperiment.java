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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.similarity.Support;
import org.urban.data.core.ngram.NGramGenerator;
import org.urban.data.provider.socrata.SocrataHelper;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class NGramExperiment {
    
    public void run(File[] files, String[] columns, BigDecimal threshold) throws java.io.IOException {
        
        NGramGenerator nGramGen = new NGramGenerator(3);
        
        HashMap<String, int[]> nGramIndex = new HashMap<>();
        
        int[] rows = new int[files.length];
        for (int iFile = 0; iFile < files.length; iFile++) {
            File file = files[iFile];
            try (CSVParser parser = SocrataHelper.tsvParser(file)) {
                for (CSVRecord row : parser) {
                    String value = row.get(columns[iFile]).toUpperCase();
                    if (!value.equals("")) {
                        for (String nGram : nGramGen.getNGrams(value)) {
                            if (!nGramIndex.containsKey(nGram)) {
                                nGramIndex.put(nGram, new int[files.length]);
                            }
                            nGramIndex.get(nGram)[iFile]++;
                        }
                    }
                    rows[iFile]++;
                }
            }
        }
        
        ArrayList<String> nGrams = new ArrayList<>(nGramIndex.keySet());
        Collections.sort(nGrams);
        
        for (String nGram : nGrams) {
            String line = nGram;
            boolean print = false;
            for (int iFile = 0; iFile < files.length; iFile++) {
                int rowCount = rows[iFile];
                int count = nGramIndex.get(nGram)[iFile];
                if (count > 0) {
                    Support sup = new Support(count, rowCount, 4);
                    line += "\t" + sup;
                    if (sup.value().compareTo(threshold) >= 0) {
                        print = true;
                    }
                } else {
                    line += "\t0";
                }
            }
            if (print) {
                System.out.println(line);
            }
        }
    }
    
    public static void main(String[] args) {
        
        DB db = new DB(new File("."));
        
        BigDecimal threshold;
        if (args.length == 1) {
            threshold = new BigDecimal(args[0]);
        } else {
            threshold = BigDecimal.ZERO;
        }
        
        File[] files = new File[3];
        files[0] = db.datasetFile(new Dataset("3h2n-5cm9", "data.cityofnewyork.us", "20190927"));
        files[1] = db.datasetFile(new Dataset("bty7-2jhb", "data.cityofnewyork.us", "20190927"));
        files[2] = db.datasetFile(new Dataset("pdiy-9ae5", "data.cityofnewyork.us", "20190927"));
        
        String[] columns = new String[]{"STREET", "Street", "STREET"};
        
        try {
            new NGramExperiment().run(files, columns, threshold);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
