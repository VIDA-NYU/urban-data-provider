/*
 * Copyright 2018 New York University.
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
package org.urban.data.provider.socrata;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.util.StringHelper;

/**
 * Query the Socrata resource catalog.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class CatalogQuery {
    
    private final File _catalogFile;
    
    public CatalogQuery(File catalogFile) {
        
        _catalogFile = catalogFile;
    }
    
    private void addPath(
            Entry<String, JsonElement> entry,
            String prefix,
            HashSet<String> schema
    ) {

        String path = prefix + "/" + entry.getKey();
        if (!schema.contains(path)) {
            schema.add(path);
        }
        
        JsonElement el = entry.getValue();
        if (el.isJsonObject()) {
            for (Entry<String, JsonElement> child : el.getAsJsonObject().entrySet()) {
                this.addPath(child, path, schema);
            }
        }
    }
    
    public List<String[]> eval(List<JQuery> select, boolean noNullValues) throws java.io.IOException {

        List<String[]> result = new ArrayList<>();
        
        JsonParser parser = new JsonParser();
        try (InputStream is = FileSystem.openFile(_catalogFile)) {
            JsonReader reader = new JsonReader(new InputStreamReader(is));
            reader.beginArray();
            while (reader.hasNext()) {
                JsonObject doc = parser.parse(reader).getAsJsonObject();
                String[] tuple = new String[select.size()];
                boolean hasNull = false;
                for (int iCol = 0; iCol < select.size(); iCol++) {
                    JsonElement el = select.get(iCol).eval(doc);
                    if (el != null) {
                        if (el.isJsonPrimitive()) {
                            tuple[iCol] = el.getAsString();
                        } else {
                            tuple[iCol] = el.toString();
                        }
                    } else {
                        tuple[iCol] = "";
                        hasNull = true;
                    }
                }
                if ((!hasNull) || (!noNullValues)) {
                    result.add(tuple);
                }
            }
            reader.endArray();
        }
        
        return result;
    }
    
    public List<String[]> eval(List<JQuery> select) throws java.io.IOException {
    
        return this.eval(select, false);
    }
    
    public void schema(PrintWriter out) throws java.io.IOException {
        
        JsonParser parser = new JsonParser();
        
        HashSet<String> schema = new HashSet();
        
        try (InputStream is = FileSystem.openFile(_catalogFile)) {
            JsonReader reader = new JsonReader(new InputStreamReader(is));
            reader.beginArray();
            while (reader.hasNext()) {
                JsonObject doc = parser.parse(reader).getAsJsonObject();
                for (Entry<String, JsonElement> entry : doc.entrySet()) {
                    this.addPath(entry, "", schema);
                }
            }
            reader.endArray();
        }
        
        List<String> paths = new ArrayList<>(schema);
        Collections.sort(paths);
        for (String path : paths) {
            out.println(path);
        }
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <catalog-file>\n" +
            "  [-s | <path-1>, ...]";
 
    public static void main(String[] args) {
        
        if (args.length < 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File catalogFile = new File(args[0]);
        
        CatalogQuery catalog = new CatalogQuery(catalogFile);
        
        try (PrintWriter out = new PrintWriter(System.out)) {
            if ((args.length == 2) && (args[1].equals("-s"))) {
                catalog.schema(out);
            } else {
                List<JQuery> select = new ArrayList<>();
                for (int iArg = 1; iArg < args.length; iArg++) {
                    select.add(new JQuery(args[iArg]));
                }
                for (String[] tuple : catalog.eval(select)) {
                    out.println(StringHelper.joinStrings(tuple, "\t"));
                }
            }
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
