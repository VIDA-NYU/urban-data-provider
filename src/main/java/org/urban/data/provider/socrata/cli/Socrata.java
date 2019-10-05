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

/**
 * Socrata command line interface.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Socrata {
    
    private static final Command[] COMMANDS = {
        new Clean(),
        new ColumnValues(),
        new DownloadDates(),
        new DownloadDatasets(),
        new DiskUsage(),
        new DatasetLoadfile(),
        new DatasetNames(),
        new DatasetSchema(),
        new DatabaseSnapshot(),
        new Parse()
    };

        private static final Logger LOGGER = Logger
            .getLogger(Socrata.class.getName());
    
    private static HashMap<String, Command> commandListing() {

        HashMap<String, Command> result = new HashMap<>();
        for (Command cmd : COMMANDS) {
            result.put(cmd.name(), cmd);
        }
        return result;
    }
    
    private static void printHelp() {
        
        ArrayList<Command> commands = new ArrayList<>(commandListing().values());
        Collections.sort(commands, (cmd1, cmd2) -> (cmd1.name().compareTo(cmd2.name())));
        Socrata.printProgramName();
        for (Command cmd : commands) {
            System.out.println();
            System.out.println(cmd.name() + ": " + cmd.shortDescription());
        }
    }
    
    private static void printHelp(Command cmd) {
        
        Socrata.printProgramName();
        System.out.println();
        System.out.println(cmd.name() + ": " + cmd.shortDescription());
        
        String description = cmd.longDescription();
        if (description != null) {
            System.out.println();
            System.out.println(description);
        }
        System.out.println();
        
        for (String key : cmd.parameters().keySet()) {
            System.out.println("--" + key + ": " + cmd.parameters().get(key));
        }
    }
    
    private static void printProgramName() {
        
        System.out.println("Socrata Data Archive - Command line Tool (Version 0.1.2)");
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
                Socrata.printHelp(command);
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
