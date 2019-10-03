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

import org.apache.commons.csv.CSVParser;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.Dataset;

/**
 * Print the name of columns for a given dataset.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatasetSchema implements Command {

    @Override
    public void help(boolean includeDescription) {

        Help.printName(this.name(), "Dataset schema");
        Help.printDir();
        Help.printDomain();
        Help.printDate("Download date (default: last)");
        Help.printDataset();
    }

    @Override
    public String name() {

        return "dataset schema";
    }

    @Override
    public void run(Args args) throws java.io.IOException {

        DB db = args.getDB();
        
        int count = 0;
        for (Dataset dataset : db.getDownloadedDatasets(args.asQuery())) {
            if (count > 0) {
                System.out.println();
            }
            System.out.println(dataset);
            try (CSVParser in = db.open(dataset)) {
                int iColumn = 1;
                for (String name : in.getHeaderNames()) {
                    System.out.println("  " + (iColumn++) + ": " + name);
                }
            }
            count++;
        }
    }
}
