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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.urban.data.core.io.FileSystem;
import org.urban.data.core.query.json.JFilter;
import org.urban.data.core.query.json.JQuery;
import org.urban.data.core.sort.NamedObjectComparator;

/**
 * Methods for downloading and querying the Socrata resource catalog.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SocrataCatalog {
    
    /**
     * Socrata app token to be included in requests.
     */
    public static final String APP_TOKEN = "e8DxKoPGZ2iU_Y2orwM9HL-cpTYrKipvdJRd";
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <catalog-file>\n" +
            "  <resource-type> [\n" +
            "    api |\n" +
            "    calendar |\n" +
            "    chart |\n" +
            "    datalens |\n" +
            "    dataset |\n" +
            "    federated_href |\n" +
            "    file |\n" +
            "    filter |\n" +
            "    form |\n" +
            "    href |\n" +
            "    link |\n" +
            "    map |\n" +
            "    measure |\n" +
            "    story |\n" +
            "    visualization\n" +
            "  ]\n" +
            "  {<domain>}";
    
    private static final Logger LOGGER = Logger.getGlobal();
    
    public static final String VERSION = "0.1.0";

    /**
     * Base URL for Socrata dataset catalog. Results are limited to a maximum
     * of 100 records. Request format for pagination is:
     * 
     * ?limit=100&offset=10000
     * 
     * Use scroll_id to retrieve results that contain more than 10000 entries.
     * 
     */
    public static final String[][] URLS = {
        {"api.us.socrata.com", "/api/catalog/v1"},
        {"api.eu.socrata.com", "/api/catalog/v1"}
    };

    /**
     * Default limit of results when downloading the catalog
     */
    public final int LIMIT = 10000;
    
    /**
     * File on local disk that contains the catalog (may be empty if catalog has
     * not been downloaded yet).
     */
    private final File _catalog;
    
    /**
     * Initialize the file on local disk that contains the catalog version. The
     * file may not exist if the catalog hasn't been downloaded yet.
     * 
     * @param catalog 
     */
    public SocrataCatalog(File catalog) {
        
        _catalog = catalog;
    }
    
    /**
     * Download catalog for all resources of given type.
     * 
     * @param type
     * @throws java.net.URISyntaxException
     * @throws java.io.IOException 
     */
    public void download(String type)  throws java.net.URISyntaxException, java.io.IOException {
        
        List<SocrataDomain> domains = SocrataCatalog.listDomains();
        
        try (JsonWriter out = new JsonWriter(
                new OutputStreamWriter(FileSystem.openOutputFile(_catalog)))
        ) {
            out.beginArray();
            for (SocrataDomain domain : domains) {
                System.out.println(domain.name());
                this.downloadAndWriteResources(domain.name(), type, out);
            }
            out.endArray();
        }
   }
    
    /**
     * Download full Socrata catalog. Write result to the catalog file.
     * 
     * @param domain
     * @param type
     * @throws java.net.URISyntaxException
     * @throws java.io.IOException 
     */
    public void download(String domain, String type) throws java.net.URISyntaxException, java.io.IOException {
        
        try (JsonWriter out = new JsonWriter(
                new OutputStreamWriter(FileSystem.openOutputFile(_catalog)))
        ) {
            out.beginArray();
            this.downloadAndWriteResources(domain, type, out);
            out.endArray();
        }
    }
    
    /**
     * Download resources of given type from catalog and write to output.
     * 
     * @param domain
     * @param type
     * @param out
     * @throws java.net.URISyntaxException
     * @throws java.io.IOException 
     */
    private void downloadAndWriteResources(String domain, String type, JsonWriter out) throws java.net.URISyntaxException, java.io.IOException {
        
        HttpClient client = HttpClientBuilder.create().build();

        Gson gson = new Gson();
        for (String[] api : URLS) {
            String scrollId = null;
            boolean done = false;
            int entryCount = 0;
            int resultSetSize = -1;
            while (!done) {
                URIBuilder uri = new URIBuilder()
                    .setScheme("http")
                    .setHost(api[0])
                    .setPath(api[1])
                    .setParameter("domains", domain)
                    .setParameter("only", type)
                    .setParameter("limit", Integer.toString(LIMIT));
                if (scrollId != null) {
                    uri.setParameter("&scroll_id=", scrollId);
                }
                System.out.println(uri.toString());
                HttpGet request = new HttpGet(uri.build());
                request.addHeader("X-App-Token", APP_TOKEN);
                HttpResponse response = client.execute(request);
                try (JsonReader reader = new JsonReader(
                    new InputStreamReader(response.getEntity().getContent(), "UTF-8"))
                ) {
                    int resultCount = 0;
                    reader.beginObject();
                    while (reader.hasNext()) {
                        String name = reader.nextName();
                        if (name.equals("results")) {
                            reader.beginArray();
                            while (reader.hasNext()) {
                                JsonObject doc = new JsonParser().parse(reader).getAsJsonObject();
                                gson.toJson(doc, out);
                                scrollId = new JQuery("resource/id").eval(doc).getAsString();
                                resultCount++;
                                entryCount++;
                            }
                            reader.endArray();
                        } else if (name.equals("resultSetSize")) {
                            resultSetSize = reader.nextInt();
                        } else if (name.equals("error")) {
                            done = true;
                            reader.skipValue();
                        } else {
                            reader.skipValue();
                        }
                    }
                    reader.endObject();
                    if (!done) {
                        done = (entryCount >= resultCount);
                    }
                }
            }
            if (entryCount > 0) {
                if (entryCount != resultSetSize) {
                    System.out.println("Got only " + entryCount + " entries of " + resultSetSize);
                } else {
                    System.out.println("Read " + entryCount + " entries");
                    break;
                }
            }
        }
    }
    
    /**
     * Get a list of all domains that are available at the Socrata API.
     * 
     * @return
     * @throws java.net.URISyntaxException 
     * @throws java.io.IOException 
     */
    public static List<SocrataDomain> listDomains() throws  java.net.URISyntaxException, java.io.IOException {
        
        List<SocrataDomain> result = new ArrayList<>();
        
        HttpClient client = HttpClientBuilder.create().build();

        for (String[] api : URLS) {
            URIBuilder uri = new URIBuilder()
                .setScheme("http")
                .setHost(api[0])
                .setPath(api[1] + "/domains");
            HttpGet request = new HttpGet(uri.build());
            request.addHeader("X-App-Token", APP_TOKEN);
            HttpResponse response = client.execute(request);
            try (JsonReader reader = new JsonReader(
                new InputStreamReader(response.getEntity().getContent(), "UTF-8"))
            ) {
                reader.beginObject();
                while (reader.hasNext()) {
                    String name = reader.nextName();
                    if (name.equals("results")) {
                        reader.beginArray();
                        while (reader.hasNext()) {
                            JsonObject doc = new JsonParser().parse(reader).getAsJsonObject();
                            if ((doc.has("thing")) && (doc.has("count"))) {
                                result.add(
                                        new SocrataDomain(
                                                doc.get("thing").getAsString(),
                                                doc.get("count").getAsInt()
                                        )
                                );
                            }
                        }
                        reader.endArray();
                    } else {
                        reader.skipValue();
                    }
                }
                reader.endObject();
            }
        }
        
        Collections.sort(result, new NamedObjectComparator<>());
        
        return result;
    }
    
    public List<String[]> query(List<JQuery> select, List<JFilter> filter) throws java.io.IOException {
        
        ArrayList<String[]> result = new ArrayList<>();
        
        try (JsonReader reader = new JsonReader(
            new InputStreamReader(FileSystem.openFile(_catalog)))
        ) {
            reader.beginArray();
            while (reader.hasNext()) {
                JsonObject doc = new JsonParser().parse(reader).getAsJsonObject();
                boolean accept = true;
                for (JFilter cond : filter) {
                    if (!cond.eval(doc)) {
                        accept = false;
                        break;
                    }
                }
                if (accept) {
                    String[] tuple = new String[select.size()];
                    for (int iCol = 0; iCol < select.size(); iCol++) {
                        tuple[iCol] = select.get(iCol).eval(doc).getAsString();
                    }
                    result.add(tuple);
                }
            }
            reader.endArray();
        }
        
        return result;
    }
    
    public List<String[]> query(List<JQuery> select) throws java.io.IOException {
        
        return this.query(select, new ArrayList<JFilter>());
    }
    
    public static void main(String[] args) {
        
	System.out.println("Urban Data Integration - Socrata Catalog - Version (" + VERSION + ")\n");

        if ((args.length < 2) || (args.length > 3)) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File outputFile = new File(args[0]);
        String type = args[1];
        String domain = null;
        if (args.length == 3) {
            domain = args[2];
        }
        
        SocrataCatalog catalog = new SocrataCatalog(outputFile);
        try {
            if (domain != null) {
                catalog.download(domain, type);
            } else {
                catalog.download(type);
            }
        } catch (java.net.URISyntaxException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
        }
    }
}
