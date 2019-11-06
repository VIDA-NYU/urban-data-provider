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
import java.util.LinkedList;
import java.util.List;
import org.urban.data.core.constraint.Threshold;
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
    
    public List<File> eval(
            List<File> queryFiles,
            List<File> databaseFiles,
            int k
    ) throws java.io.IOException {
        
        
        HashMap<String, ColumnValues> query = new HashMap<>();
        HashMap<String, LinkedList<ColumnFileScore>> scores = new HashMap<>();
        for (File file : queryFiles) {
            ColumnValues col = new ColumnValues(file, _nGramThreshold);
            query.put(file.getAbsolutePath(), col);
            scores.put(col.filePath(), new LinkedList<>());
        }
        
        System.out.println("QUERY HAS " + query.size() + " OF " + queryFiles.size() + " FILES");
        
        for (File file : databaseFiles) {
            if (!query.containsKey(file.getAbsolutePath())) {
                ColumnValues dbCol = new ColumnValues(file, _nGramThreshold);
                for (ColumnValues qCol : query.values()) {
                    BigDecimal ji = qCol.ji(dbCol);
                    if (ji.compareTo(BigDecimal.ZERO) > 0) {
                        BigDecimal jsd = BigDecimal.ONE.subtract(qCol.jsd(dbCol));
                        if (jsd.compareTo(BigDecimal.ZERO) > 0) {
                            BigDecimal max = new BigDecimal(Math.max(ji.doubleValue(), jsd.doubleValue()));
                            scores.get(qCol.filePath()).add(new ColumnFileScore(file, max));
                            String line = qCol.name() + "\t" + dbCol.name();
                            line += "\t" + new FormatedBigDecimal(ji);
                            line += "\t" + new FormatedBigDecimal(jsd);
                            line += "\t" + new FormatedBigDecimal(max);
                            System.out.println(line);
                        }
                    }
                }
            }
        }
        
        List<LinkedList<ColumnFileScore>> candidates = new ArrayList<>();
        for (LinkedList<ColumnFileScore> score : scores.values()) {
            if (!score.isEmpty()) {
                Collections.sort(score);
                Collections.reverse(score);
                candidates.add(score);
            }
        }
        
        HashMap<String, File> result = new HashMap<>();
        int index = 0;
        while ((!candidates.isEmpty()) && ((result.size() < k))) {
            LinkedList<ColumnFileScore> candScores = candidates.get(index);
            ColumnFileScore score = candScores.pop();
            String key = score.file().getAbsolutePath();
            if (!result.containsKey(key)) {
                result.put(key, score.file());
                if (result.size() >= k) {
                    break;
                }
            }
            if (candScores.isEmpty()) {
                candidates.remove(index);
            } else {
                index++;
            }
            if (index >= candidates.size()) {
                index = 0;
            }
        }
        return new ArrayList<>(result.values());
    }
}
