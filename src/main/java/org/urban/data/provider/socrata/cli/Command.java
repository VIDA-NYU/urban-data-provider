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

/**
 * Module that can be run as a Socrata CLI command.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public interface Command {
   
    /**
     * Print help statement for this command. The command description is only
     * printed if the respective flag is true.
     * 
     * @param includeDescription
     */
    public void help(boolean includeDescription);
    
    /**
     * Get the command name.
     * 
     * @return 
     */
    public String name();
    
    /**
     * Run the command for the given set of arguments.
     * 
     * @param args
     * @throws java.io.IOException 
     */
    public void run(Args args) throws java.io.IOException;
}
