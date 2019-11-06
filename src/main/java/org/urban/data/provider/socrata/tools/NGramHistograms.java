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
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.constraint.Threshold;
import org.urban.data.core.util.FormatedBigDecimal;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class NGramHistograms {
    
    public void run(File file1, File file2, Threshold threshold) throws java.io.IOException {
        
        ColumnValues col1 = new ColumnValues(file1, threshold);
        ColumnValues col2 = new ColumnValues(file2, threshold);
        
        HashSet<String> keys = new HashSet<>(col1.ngrams().keySet());
        for (String key : col2.ngrams().keySet()) {
            if (!keys.contains(key)) {
                keys.add(key);
            }
        }
        ArrayList<String> ngrams = new ArrayList<>(keys);
        Collections.sort(ngrams);
        
        for (String ngram : ngrams) {
            FormatedBigDecimal v1;
            if (col1.ngrams().containsKey(ngram)) {
                v1 = new FormatedBigDecimal(col1.ngrams().get(ngram));
            } else {
                v1 = new FormatedBigDecimal(BigDecimal.ZERO);
            }
            FormatedBigDecimal v2;
            if (col2.ngrams().containsKey(ngram)) {
                v2 = new FormatedBigDecimal(col2.ngrams().get(ngram));
            } else {
                v2 = new FormatedBigDecimal(BigDecimal.ZERO);
            }
            System.out.println(ngram + "\t" + v1 + "\t" + v2);
        }
        
        System.out.println("JSD: " + new FormatedBigDecimal(col1.jsd(col2)));
    }
    
    private static final String COMMAND = "Usage <file-1> <file-2> <threshold>";
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File file1 = new File(args[0]);
        File file2 = new File(args[1]);
        Threshold threshold = Threshold.getConstraint(args[2]);
        
        try {
            new NGramHistograms().run(file1, file2, threshold);
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
