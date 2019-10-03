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

import java.io.IOException;
import org.urban.data.provider.socrata.db.DB;
import org.urban.data.provider.socrata.db.DatabaseLoader;
import org.urban.data.provider.socrata.db.Dataset;

/**
 * Create load file for a given dataset.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatasetLoadfile implements Command {
   
    @Override
    public void help(boolean includeDescription) {

        Help.printName(this.name(), "Create load file and statement for dataset");
        Help.printDir();
        Help.printDomain();
        Help.printDataset();
        Help.printDate("Download date (default: today)");
        Help.printOutput("Output directory");
    }

    @Override
    public String name() {

        return "dataset loadfile";
    }
   
    @Override
    public void run(Args args) throws IOException {

        if (!args.hasOutput()) {
            throw new IllegalArgumentException("No output directory given");
        }
        
        DB db = args.getDB();
        DatabaseLoader loader = new DatabaseLoader(db);

        for (Dataset dataset : db.getDownloadedDatasets(args.asQuery())) {
            loader.createLoadFile(dataset, args.getOutput());
        }
    }
}
