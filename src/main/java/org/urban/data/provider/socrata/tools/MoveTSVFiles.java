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
import java.util.ArrayList;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MoveTSVFiles {
    
    public static void main(String[] args) {
        
        System.out.println("#!/bin/sh\n");
        File baseDir = new File(".");
        for (File domainDir : baseDir.listFiles()) {
            if (domainDir.isDirectory()) {
                for (File dateDir : domainDir.listFiles()) {
                    if (dateDir.isDirectory()) {
                        String path = "./" + domainDir.getName() + "/" + dateDir.getName();
                        ArrayList<File> datasets = new ArrayList<>();
                        for (File file : dateDir.listFiles()) {
                            if (file.getName().endsWith(".tsv.gz")) {
                                datasets.add(file);
                            }
                        }
                        if (!datasets.isEmpty()) {
                            String targetPath = path + "/tsv";
                            if (!new File(targetPath).exists()) {
                                System.out.println("mkdir " + targetPath + ";");
                            }
                            for (File file : datasets) {
                                String dsPath = path + "/" + file.getName();
                                System.out.println("mv " + dsPath + " " + targetPath + ";");
                            }
                        }
                    }
                }
            }
        }
    }
}
