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

import org.urban.data.core.util.StringHelper;

/**
 * Helper methods to print help statements for different command line arguments.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public final class Help {
   
    public static void printColumn() {
        
        System.out.println(" --" + Args.PARA_COLUMN + "  : Column name or index");
    }
   
    public static void printDate(String text) {
        
        System.out.println(" --" + Args.PARA_DATE + "    : " + text);
    }
   
    public static void printDataset() {
        
        System.out.println(" --" + Args.PARA_DATASET + " : Unique dataset identifier");
    }
   
    public static void printDir() {
        
        System.out.println(" --" + Args.PARA_BASEDIR + "     : Base directory for the archive");
    }
   
    public static void printDescription(String text) {
        
        System.out.println(text);
        System.out.println();
    }
    
    public static void printDomain() {
        
        System.out.println(" --" + Args.PARA_DOMAIN + "  : Unique domain name");
    }
   
    public static void printHtml() {
        
        System.out.println(" --" + Args.PARA_HTML + "    : Delete (potential) HTML files");
    }
   
    public static void printName(String name, String description) {
        
        System.out.println(name + StringHelper.repeat(" ", 11 - name.length()) + ": " + description);
        System.out.println();
    }
   
    public static void printOrderBy() {
        
        System.out.println(" --" + Args.PARA_ORDERBY + "    : Order by value of count");
    }
   
    public static void printOutput(String text) {
        
        System.out.println(" --" + Args.PARA_OUTPUT + "  : " + text);
    }
   
    public static void printReport() {
        
        System.out.println(" --" + Args.PARA_REPORT + "  : Print actions but do not execute");
    }
   
    public static void printReverse() {
        
        System.out.println(" --" + Args.PARA_REVERSE + "  : Reverse default output order");
    }
   
    public static void printStatsOnly() {
        
        System.out.println(" --" + Args.PARA_STATS + "  : Only output statistics");
    }
   
    public static void printThreads() {
        
        System.out.println(" --" + Args.PARA_THREADS + " : Number of parallel threads used");
    }
}
