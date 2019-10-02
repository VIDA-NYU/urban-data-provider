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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.urban.data.provider.socrata.cli.Args;
import org.urban.data.provider.socrata.cli.Command;
import org.urban.data.provider.socrata.cli.Help;

/**
 * Print download dates and number of files. Prints a listing of all dates on
 * which datasets have been downloaded together with the number of downloaded
 * files.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DateListingPrinter implements Command {


    @Override
    public void help() {

        Help.printName(this.name(), "Download dates");
        Help.printDir();
    }

    @Override
    public String name() {

        return "dates";
    }

    @Override
    public void run(Args args) throws java.io.IOException {

        HashMap<String, Integer> stats = args.getDB().getDateStats();
        
        ArrayList<String> dates = new ArrayList<>(stats.keySet());
        Collections.sort(dates);
        
        for (String date : dates) {
            System.out.println(date + " : " + stats.get(date));
        }
    }
}
