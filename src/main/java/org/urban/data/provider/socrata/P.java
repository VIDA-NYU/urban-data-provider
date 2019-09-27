/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.urban.data.provider.socrata;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class P {
    public static void main(String[] args) {
        String link = "https://agtransport.usda.gov/d/g9w7-d2kh";
        
        String domain = "agtransport.usda.gov";
        String permalink = "https://agtransport.usda.gov/d/g9w7-d2kh";
        if (permalink.contains("/d/")) {
            String url = permalink.replace("/d/", "/api/views/");
            url += "/rows.tsv?accessType=DOWNLOAD";
            System.out.println(url);
        }
    }
}
