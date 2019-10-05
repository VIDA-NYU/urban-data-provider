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
package org.urban.data.provider.socrata.cli;

import java.util.List;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.urban.data.core.value.ValueCounter;
import org.urban.data.core.value.ValueIndex;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;

/**
 * List value and frequency for the distinct terms in a dataset column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnValues extends CommandImpl implements Command {

    public ColumnValues() {

        super("column values", "Dataset schema");
        this.addParameter(Args.PARA_DOMAIN);
        this.addParameter(Args.PARA_DATE, "Download date (default: last)");
        this.addParameter(Args.PARA_DATASET);
        this.addParameter(Args.PARA_COLUMN);
        this.addParameter(Args.PARA_ORDERBY);
        this.addParameter(Args.PARA_REVERSE);
    }

    @Override
    public void run(Args args) throws java.io.IOException {

        DB db = args.getDB();
        
        if (!args.hasColumn()) {
            throw new IllegalArgumentException("No column specififed");
        }
        String columnPara = args.getColumn();
        int columnIndex;
        try {
            columnIndex = Integer.parseInt(columnPara) - 1;
        } catch (java.lang.NumberFormatException ex) {
            columnIndex = -1;
        }

        for (Dataset dataset : db.getSnapshot(args.asQuery())) {
            try (CSVParser in = db.open(dataset)) {
                if (columnIndex == -1) {
                    if (in.getHeaderMap().containsKey(columnPara)) {
                        columnIndex = in.getHeaderMap().get(columnPara);
                    } else {
                        continue;
                    }
                }
                ValueIndex values = new ValueIndex();
                for (CSVRecord record : in) {
                    values.add(record.get(columnIndex));
                }
                List<ValueCounter> valueList;
                if (args.getOrderBy().equalsIgnoreCase(Args.ORDER_BY_COUNT)) {
                    valueList = values.listByFrequency(args.getReverse());
                } else {
                    valueList = values.listAlphabetical(args.getReverse());
                }
                for (ValueCounter value : valueList) {
                    System.out.println(value.getText() + "\t" + value.getCount());
                }
            }
        }
    }
}
