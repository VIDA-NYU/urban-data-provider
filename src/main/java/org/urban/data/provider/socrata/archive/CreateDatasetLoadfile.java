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
package org.urban.data.provider.socrata.archive;

import org.urban.data.provider.socrata.cli.Args;
import java.io.IOException;
import org.urban.data.provider.socrata.cli.Command;
import org.urban.data.provider.socrata.cli.Help;

/**
 * Create load file for a given dataset.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CreateDatasetLoadfile implements Command {
   
    @Override
    public void help() {

        Help.printName(this.name(), "Create load file and statement for dataset");
        Help.printDir();
        Help.printDomain();
        Help.printDataset();
        Help.printDate("Download date (default: today)");
    }

    @Override
    public String name() {

        return "load";
    }
   
    @Override
    public void run(Args args) throws IOException {

        if (!args.hasDataset()) {
            throw new IllegalArgumentException("No dataset given");
        }
        if (!args.hasDomain()) {
            throw new IllegalArgumentException("No domain given");
        }
        
        Dataset dataset = new Dataset(
                args.getDataset(),
                args.getDomain(),
                args.getDate()
        );
        
        DatabaseLoader loader = new DatabaseLoader(args.getDB());
        loader.createLoadFile(dataset, args.getOutput());
    }
}
