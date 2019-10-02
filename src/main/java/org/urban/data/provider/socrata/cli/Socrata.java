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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.provider.socrata.archive.CleanEmptyFilesAndHTML;
import org.urban.data.provider.socrata.archive.CreateDatasetLoadfile;
import org.urban.data.provider.socrata.archive.DatasetNameQuery;
import org.urban.data.provider.socrata.archive.DatasetStatsWriter;
import org.urban.data.provider.socrata.archive.DateListingPrinter;
import org.urban.data.provider.socrata.archive.DiskUsageStatsPrinter;
import org.urban.data.provider.socrata.archive.UpdatedDatasetDownloader;

/**
 * Socrata command line interface.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Socrata {
    
    private static final Command[] COMMANDS = {
        new CleanEmptyFilesAndHTML(),
        new DateListingPrinter(),
        new UpdatedDatasetDownloader(),
        new DiskUsageStatsPrinter(),
        new CreateDatasetLoadfile(),
        new DatasetNameQuery(),
        new DatasetStatsWriter()
    };

        private static final Logger LOGGER = Logger
            .getLogger(Socrata.class.getName());
    
    public static HashMap<String, Command> commandListing() {

        HashMap<String, Command> result = new HashMap<>();
        for (Command cmd : COMMANDS) {
            result.put(cmd.name(), cmd);
        }
        return result;
    }
    
    public static void printHelp() {
        
        ArrayList<Command> commands = new ArrayList<>(commandListing().values());
        Collections.sort(commands, (cmd1, cmd2) -> (cmd1.name().compareTo(cmd2.name())));
        System.out.println("Socrata Data Archive - Command line Tool (Version 0.1.0)");
        for (Command cmd : commands) {
            System.out.println();
            cmd.help();
        }
    }
    
    public static void main(String[] arguments) {

        if (arguments.length == 0) {
            printHelp();
            System.exit(-1);
        }
        
        Args args = new Args(arguments);
        
        Command command = commandListing().get(args.command());
        if (command == null) {
            System.out.println("Unknown command: " + args.command());
            System.exit(-1);
        }
        
        if (args.hasHelp()) {
            if (args.getHelp()) {
                command.help();
                System.exit(0);
            }
        }
        
        try {
            command.run(args);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }        
    }
}
