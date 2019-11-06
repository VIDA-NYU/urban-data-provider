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

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnFileScore implements Comparable<ColumnFileScore> {
    
    private final File _file;
    private final BigDecimal _value;

    public ColumnFileScore(File file, BigDecimal value) {

        _file = file;
        _value = value;
    }

    @Override
    public int compareTo(ColumnFileScore s) {

        return _value.compareTo(s.value());
    }

    public File file() {

        return _file;
    }

    public BigDecimal value() {

        return _value;
    }
}
