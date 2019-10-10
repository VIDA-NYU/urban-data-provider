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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.urban.data.core.util.count.Counter;
import org.urban.data.provider.socrata.db.DatabaseLoader;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SQLEscapeTest {
    
    public SQLEscapeTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testEscapeTerms() {
    
        String term = "D'LILI BAKERY\\DOWN THE ROAD";
        String value = new DatabaseLoader(null).escapeTerm(term, new Counter());
        assertEquals("D''LILI BAKERY\\\\DOWN THE ROAD", value);
        
        term = "RM    \\\t";
        value = new DatabaseLoader(null).escapeTerm(term, new Counter());
        assertEquals("RM    \\\\\t", value);
        
        term = "RM    \\\nD'LILI BAKERY\\DOWN THE ROAD";
        value = new DatabaseLoader(null).escapeTerm(term, new Counter());
        assertEquals("RM    \\\\ D''LILI BAKERY\\\\DOWN THE ROAD", value);
    }
    
    @Test
    public void testReplaceSpecialChars() {
        
        String name = 
                "SERVICE CATEGORY\n" +
                "(0K-08* ONLY)";
        name = DatabaseLoader.replaceSpecialChars(name);
        assertEquals("SERVICE_CATEGORY__0K_08__ONLY_", name);
    }
}
