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

import java.math.BigDecimal;
import org.urban.data.provider.socrata.db.Dataset;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class TopKListEntry implements Comparable<TopKListEntry> {
    
    private final String _column;
    private final Dataset _dataset;
    private final BigDecimal _sim;

    public TopKListEntry(Dataset dataset, String column, BigDecimal sim) {

        _dataset = dataset;
        _column = column;
        _sim = sim;
    }

    public String column() {
        
        return _column;
    }
    
    @Override
    public int compareTo(TopKListEntry entry) {

        return _sim.compareTo(entry.sim());
    }

    public Dataset dataset() {

        return _dataset;
    }

    public BigDecimal sim() {

        return _sim;
    }
}
