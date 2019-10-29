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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.urban.data.core.constraint.Threshold;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.ngram.NGramGenerator;
import org.urban.data.core.set.StringSet;
import org.urban.data.core.similarity.JaccardIndex;
import org.urban.data.core.similarity.JensenShannonDivergence;
import org.urban.data.core.similarity.Support;
import org.urban.data.core.util.count.Counter;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnValues {
    
    private final File _file;
    private final HashMap<String, BigDecimal> _nGrams;
    private final StringSet _values;

    public ColumnValues(File file, Threshold threshold) throws java.io.IOException {
        
        _file = file;

        _values = new StringSet();
        HashMap<String, Counter> nGramIndex = new HashMap<>();

        NGramGenerator ngrams = new NGramGenerator(3);
        
        try (BufferedReader in = FileSystem.openReader(file)) {
            String line;
            while ((line = in.readLine()) != null) {
                String term = line.split("\t")[0];
                _values.add(term);
                for (String nGram : ngrams.getNGrams(term)) {
                    if (!nGramIndex.containsKey(nGram)) {
                        nGramIndex.put(nGram, new Counter(1));
                    } else {
                        nGramIndex.get(nGram).inc();
                    }
                }
            }
        }
        
        _nGrams = new HashMap<>();
        for (String nGram : nGramIndex.keySet()) {
            int count = nGramIndex.get(nGram).value();
            BigDecimal sup = new Support(count, _values.length()).value();
            if (threshold.isSatisfied(sup)) {
                _nGrams.put(nGram, sup);
            }
        }
    }

    public boolean equals(ColumnValues column) {

        return this.filePath().equals(column.filePath());
    }

    public String filePath() {

        return _file.getAbsolutePath();
    }

    public BigDecimal ji(ColumnValues column) {

        int ovp = _values.overlap(column.values());
        if (ovp > 0) {
            return JaccardIndex.ji(_values.length(), column.length(), ovp);
        } else {
            return BigDecimal.ZERO;
        }
    }

    public BigDecimal jsd(ColumnValues column) {

        HashSet<String> keys = new HashSet<>(_nGrams.keySet());
        for (String key : column.ngrams().keySet()) {
            if (!keys.contains(key)) {
                keys.add(key);
            }
        }

        List<String> ngrams = new ArrayList<>(keys);
        Collections.sort(ngrams);

        double[] p1 = new double[ngrams.size()];
        double[] p2 = new double[ngrams.size()];

        for (int iKey = 0; iKey < ngrams.size(); iKey++) {
            String ngram = ngrams.get(iKey);
            //String line = ngram;
            if (_nGrams.containsKey(ngram)) {
                BigDecimal sim = _nGrams.get(ngram);
                //line += "\t" + new FormatedBigDecimal(sim, 6);
                p1[iKey] = sim.doubleValue();
            } else {
                p1[iKey] = 0;
                //line += "\t0.0";
            }
            if (column.ngrams().containsKey(ngram)) {
                BigDecimal sim = column.ngrams().get(ngram);
                //line += "\t" + new FormatedBigDecimal(sim, 6);
                p2[iKey] = sim.doubleValue();
            } else {
                p2[iKey] = 0;
                //line += "\t0.0";
            }
            //System.out.println(line);
        }

        double jsd = Math.sqrt(JensenShannonDivergence.jsd(p1, p2));
        //System.out.println("\n" + jsd);
        return new BigDecimal(jsd);
    }

    public int length() {

        return _values.length();
    }

    public String name() {

        String name = _file.getName();
        if (name.endsWith(".txt.gz")) {
            name = name.substring(0, name.length() - 7);
        }
        return name;
    }

    public HashMap<String, BigDecimal> ngrams() {

        return _nGrams;
    }

    public StringSet values() {

        return _values;
    }
}
