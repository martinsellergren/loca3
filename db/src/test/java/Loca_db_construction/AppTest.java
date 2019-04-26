/**
 * Test the process of transfering data from nom to loca db.
 */
package Loca_db_construction;

import org.junit.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.List;
import java.util.ArrayList;
import org.postgis.*;

public class AppTest {

    private static Statement nomTestDb;
    private static Statement locaTestDb;

// * Tests

// ** Basic tests (null, order..)

    @Test
    public void emptyNomDb() throws SQLException {
        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(0, locaSize());
    }

    @Test
    public void singleElement() throws SQLException {
        Double importance = 0d;
        Integer rank_search = 20;
        long osm_id = 27527229;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        String name = name("Östermalm");
        Integer admin_level = 15;
        String wikidata = wikidata("Q29024431");
        String geometry = point();
        String wikipedia = "sv:Östermalm,_Västerås";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);
        App.fillLocaDb(nomTestDb, locaTestDb);

        ResultSet rs = locaElems();
        rs.next();
        assertEquals(1, rs.getLong("popindex"));
        assertEquals("Östermalm", rs.getString("name"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("place", rs.getString("subcat"));
        assertNotNull(getGeometry(rs));
        assertEquals(0, rs.getDouble("area"), 0.00001);
        assertEquals("N27527229", rs.getString("osm_ids"));
        assertEquals(1, locaSize());
        rs.close();
    }

    @Test
    public void nulls() throws SQLException {
        Double importance = null;
        Integer rank_search = null;
        String name = null;
        Integer admin_level = null;
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        name = name("");
        osm_id = 2;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        name = name("3");
        osm_id = 3;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = null;
        rank_search = 20;
        name = name("4");
        osm_id = 4;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.5;
        name = name("5");
        osm_id = 5;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.6;
        name = name("6");
        osm_id = 6;
        class_ = "boundary";
        type = "administrative";
        admin_level = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.7;
        name = name("7");
        osm_id = 7;
        class_ = "boundary";
        type = "administrative";
        admin_level = 4;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(5, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals("7", rs.getString("name"));
        assertEquals(1, rs.getLong("popindex"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("state", rs.getString("subcat"));

        rs.next();
        assertEquals("6", rs.getString("name"));
        assertEquals(2, rs.getLong("popindex"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("border", rs.getString("subcat"));

        rs.next();
        assertEquals("5", rs.getString("name"));
        assertEquals(3, rs.getLong("popindex"));

        rs.next();
        assertEquals("4", rs.getString("name"));
        assertEquals(4, rs.getLong("popindex"));

        rs.next();
        assertEquals("3", rs.getString("name"));
        assertEquals(5, rs.getLong("popindex"));

        rs.close();
    }

    @Test
    public void locaOrder() throws SQLException {
        Double importance = 1d;
        Integer rank_search = null;
        String name = name("mid");
        Integer admin_level = null;
        long osm_id = 50;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0d; //lowest
        rank_search = 2; //highest
        name = name("last");
        osm_id = 100;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 1d; //highest
        rank_search = 30; //lowest
        name = name("first");
        osm_id = 0;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(3, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals("first", rs.getString("name"));
        rs.next();
        assertEquals("mid", rs.getString("name"));
        rs.next();
        assertEquals("last", rs.getString("name"));
        rs.close();
    }

// ** Multiple entires, same elem -tests

    @Test
    public void sameOsm() throws SQLException {
        Double importance = null;
        Integer rank_search = null;
        Integer admin_level = null;
        String name = name("1N");
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        name = name("1W");
        osm_id = 1;
        osm_type = 'W';
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        name = name("2N");
        osm_id = 2;
        osm_type = 'N';
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        name = name("2W");
        osm_id = 2;
        osm_type = 'W';
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        name = name("1N duplicate");
        osm_id = 1;
        osm_type = 'N';
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(4, locaSize());
    }

    @Test
    public void sameWikidata() throws SQLException {
        Double importance = null;
        Integer rank_search = null;
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        String name = name("1");
        Integer admin_level = null;
        String wikidata = wikidata("1");
        String geometry = point(0,0);
        String wikipedia = "1";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        name = name("2");
        wikidata = wikidata("1");
        wikipedia = "2";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 3;
        name = name("3");
        wikidata = wikidata("3");
        wikipedia = "1";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = 1d;
        osm_id = 4;
        name = name("4");
        wikidata = wikidata("1");
        wikipedia = "1";
        geometry = point(1,1);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = 0.9;
        osm_id = 5;
        name = name("5");
        wikidata = wikidata("5");
        wikipedia = "5";
        geometry = point(0,0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(2, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals("4", rs.getString("name"));
        assertEquals(1, rs.getLong("popindex"));
        assertEquals(1, getGeometry(rs).getFirstPoint().x, 0.00001);
        assertEquals(1, getGeometry(rs).getFirstPoint().y, 0.00001);

        rs.close();
    }

    @Test
    public void sameAll() throws SQLException {
        Double importance = null;
        Integer rank_search = null;
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        String name = name("1");
        Integer admin_level = null;
        String wikidata = wikidata("1");
        String geometry = point(0,0);
        String wikipedia = "1";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 1;
        wikidata = wikidata("2");
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        wikidata = wikidata("1");
        wikipedia = "2";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(1, locaSize());
    }

// ** Pick geometry tests

    @Test
    public void pickGeometry_nodes() throws SQLException {
        Double importance = null;
        Integer rank_search = null;
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        String name = name("1");
        String geometry = point(1,1);
        String wikipedia = "1";
        String wikidata = null;
        Integer admin_level = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        wikipedia = "1";
        geometry = point(2,2);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = 1d;
        osm_id = 3;
        geometry = point(3,33);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = 0.1;
        osm_id = 4;
        geometry = point(4,4);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(1, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals(3, getGeometry(rs).getFirstPoint().x, 0.0000001);
        assertEquals(33, getGeometry(rs).getFirstPoint().y, 0.0000001);
        assertEquals("N3", rs.getString("osm_ids"));

        rs.close();
    }

    @Test
    public void pickGeometry_notNode() throws SQLException {
        Double importance = 1d;
        Integer rank_search = null;
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        String name = name("1");
        String geometry = point(1,1);
        String wikidata = null;
        Integer admin_level = null;
        String wikipedia = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = null;
        geometry = linestring(2,2,22,22);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = 0.5d;
        geometry = linestring(3,3,33,33);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = 0.1;
        geometry = linestring(4,4,44,44);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(1, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals(3, getGeometry(rs).getFirstPoint().x, 0.0000001);

        rs.close();
    }

// ** Test tags to category conversion

    @Test
    public void keyNotInKeyConversionTable() throws SQLException {
        Double importance = 1d;
        Integer rank_search = null;
        Integer admin_level = null;
        String name = name("1");
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "XXXXXXX";
        String type = "XXXXXX";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        boolean failed = false;
        try {
            App.fillLocaDb(nomTestDb, locaTestDb);
        }
        catch (Exception e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void keyConversion() throws SQLException {
        Double importance = 1d;
        Integer rank_search = null;
        Integer admin_level = null;
        String name = name("1");
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "aerialway";
        String type = "XXXXXX";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.9;
        name = name("2");
        osm_id = 2;
        class_ = "club";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.8;
        name = name("3");
        osm_id = 3;
        class_ = "highway";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.7;
        name = name("4");
        osm_id = 4;
        class_ = "natural";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(4, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals("1", rs.getString("name"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("aerialway", rs.getString("subcat"));

        rs.next();
        assertEquals("2", rs.getString("name"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("club hangout", rs.getString("subcat"));

        rs.next();
        assertEquals("3", rs.getString("name"));
        assertEquals("r", rs.getString("supercat"));
        assertEquals("highway", rs.getString("subcat"));

        rs.next();
        assertEquals("4", rs.getString("name"));
        assertEquals("n", rs.getString("supercat"));
        assertEquals("natural", rs.getString("subcat"));

        rs.close();
    }

    @Test
    public void keyValueConversion() throws SQLException {
        Double importance = 1d;
        Integer rank_search = null;
        Integer admin_level = null;
        String name = name("1");
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "waterway";
        String type = "stream";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.9;
        name = name("2");
        osm_id = 2;
        class_ = "amenity";
        type = "place_of_worship";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.8;
        name = name("3");
        osm_id = 3;
        class_ = "place";
        type = "islet";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.7;
        name = name("4");
        osm_id = 4;
        class_ = "boundary";
        type = "national_park";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(4, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals("1", rs.getString("name"));
        assertEquals("n", rs.getString("supercat"));
        assertEquals("waterway", rs.getString("subcat"));

        rs.next();
        assertEquals("2", rs.getString("name"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("place of worship", rs.getString("subcat"));

        rs.next();
        assertEquals("3", rs.getString("name"));
        assertEquals("n", rs.getString("supercat"));
        assertEquals("island", rs.getString("subcat"));

        rs.next();
        assertEquals("4", rs.getString("name"));
        assertEquals("n", rs.getString("supercat"));
        assertEquals("national park", rs.getString("subcat"));

        rs.close();
    }

    @Test
    public void adminBoundaryConversion() throws SQLException {
        Double importance = 1d;
        Integer rank_search = null;
        String name = name("1");
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "boundary";
        String type = "administrative";
        Integer admin_level = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.9;
        name = name("2");
        osm_id = 2;
        admin_level = 0;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.8;
        name = name("3");
        osm_id = 3;
        admin_level = 1;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.7;
        name = name("4");
        osm_id = 4;
        admin_level = 8;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(4, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals("1", rs.getString("name"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("border", rs.getString("subcat"));

        rs.next();
        assertEquals("2", rs.getString("name"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("border", rs.getString("subcat"));

        rs.next();
        assertEquals("3", rs.getString("name"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("continent", rs.getString("subcat"));

        rs.next();
        assertEquals("4", rs.getString("name"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("city", rs.getString("subcat"));

        rs.close();
    }

    @Test
    public void pickTag() throws SQLException {
        Double importance = 1d;
        Integer rank_search = null;
        String name = name("1");
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "landuse";
        String type = "xxx";
        Integer admin_level = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);
        class_ = "waterway";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);
        class_ = "highway";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        importance = 0.9;
        name = name("2");
        osm_id = 2;
        class_ = "historic";
        type = "xxx";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);
        class_ = "waterway";
        type = "waterfall";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);
        class_ = "boundary";
        type = "administrative";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(2, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals("1", rs.getString("name"));
        assertEquals("highway", rs.getString("subcat"));

        rs.next();
        assertEquals("2", rs.getString("name"));
        assertEquals("waterfall", rs.getString("subcat"));

        rs.close();
    }



// ** Test deduping
// *** Not line-strings (no deduping)
    @Test
    public void dedupe_notLinestrings() throws SQLException {
        Double importance = null;
        Integer rank_search = null;
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        String name = name("1");
        String geometry = point();
        String wikidata = null;
        Integer admin_level = null;
        String wikipedia = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 3;
        geometry = polygon(1,1,2,1,2,2,1,2);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(3, locaSize());
    }
// *** Dedupe simple objects

    @Test
    public void dedupe_singles_OUT_MERGE_DISTANCE() throws SQLException {
        double D = 101; //OUT_MERGE_DISTANCE < MAX_DEDUPE_DISTANCE
        double importance = 1.0;
        long osm_id = 1;
        String name = name("1");
        String geometry = linestring(-1, 0, 0, 0);
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        Integer rank_search = null;
        String wikidata = null;
        Integer admin_level = null;
        String wikipedia = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        geometry = linestring(mToDegAtEquator(D), 0, 1, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(2, locaSize());
    }

    @Test
    public void dedupe_singles_IN_MERGE_DISTANCE() throws SQLException {
        double D = 99; //IN_MERGE_DISTANCE < MAX_DEDUPE_DISTANCE
        double importance = 1.0;
        long osm_id = 1;

// **** 1. 2 horizontal end-start connected lines
        importance -= 0.01;
        String name = name("1");
        String geometry = linestring(-1, 0, 0, 0);
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        Integer rank_search = null;
        String wikidata = null;
        Integer admin_level = null;
        String wikipedia = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(0, 0, 1, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 2. 2 horizontal short lines with end-start space within merge
        name = name("2");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(-0.000001, 0, 0, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(mToDegAtEquator(D), 0, 0.000001+mToDegAtEquator(D), 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 3. 2 horizontal long lines with end-start space within merge
        name = name("3");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(-3, 0, 0, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(mToDegAtEquator(D), 0, 3, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 4. 2 ortogonal end-start connected lines
        name = name("4");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(-1, 0, 0, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(0, 0, 0, 1);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 5. 2 ortogonal mid-start connected lines
        name = name("5");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(-1, 0, 1, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(0, 0, 0, 1);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 6. 2 ortogonal lines with mid-start space within merge
        name = name("6");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(0, -1, 0, 1);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(mToDegAtEquator(D), 0, 1, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);


// **** 7. 2 ortogonal lines with mid-end space within merge
        name = name("7");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(0, -1, 0, 1);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(1, 0, mToDegAtEquator(D), 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 8. 2 ortogonal lines connected mid-mid (a cross)
        name = name("8");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(-1, -1, 1, 1);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(-1, 1, 1, -1);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 9. 2 equal lines
        name = name("9");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(0, 0, 1, 1);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(1, 1, 0, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 10. 3 horizontal end-start connected lines
        name = name("10");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(0, 0, 1, 1);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(1, 1, 2, 2);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(2, 2, 3, 3);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 11. 2 horizontal end-start connected lines, 1 ortogonal within merge distance
        name = name("11");
        importance -= 0.01;
        osm_id += 1;
        geometry = linestring(-2, 0, -1, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(-1, 0, 0, 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id += 1;
        geometry = linestring(mToDegAtEquator(D), 1, mToDegAtEquator(D), 0);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 0. One important, one not
        name = name("0");
        importance = 0;
        osm_id += 1;
        geometry = linestring();
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = 1;
        osm_id += 1;
        class_ = "place";
        type = "country";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** Assertions
        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(12, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals("0", rs.getString("name"));
        assertEquals(1, rs.getLong("popindex"));
        assertEquals("country", rs.getString("subcat"));
        assertEquals(String.format("N%s,N%s", osm_id, osm_id-1), rs.getString("osm_ids"));

        rs.next();
        assertEquals("1", rs.getString("name"));
        MultiLineString lines = (MultiLineString)getGeometry(rs);
        assertEquals(2, lines.numGeoms());
        assertEquals(4, lines.numPoints());

        rs.next();
        assertEquals("2", rs.getString("name"));
        lines = (MultiLineString)getGeometry(rs);
        assertEquals(2, lines.numGeoms());

        rs.next();
        assertEquals("3", rs.getString("name"));
        lines = (MultiLineString)getGeometry(rs);
        assertEquals(2, lines.numGeoms());

        rs.next();
        assertEquals("4", rs.getString("name"));
        lines = (MultiLineString)getGeometry(rs);
        assertEquals(2, lines.numGeoms());

        rs.next();
        assertEquals("5", rs.getString("name"));
        lines = (MultiLineString)getGeometry(rs);
        assertEquals(3, lines.numGeoms());

        rs.next();
        assertEquals("6", rs.getString("name"));
        lines = (MultiLineString)getGeometry(rs);
        assertEquals(2, lines.numGeoms());

        rs.next();
        assertEquals("7", rs.getString("name"));
        lines = (MultiLineString)getGeometry(rs);
        assertEquals(2, lines.numGeoms());

        rs.next();
        assertEquals("8", rs.getString("name"));
        lines = (MultiLineString)getGeometry(rs);

        rs.next();
        assertEquals("9", rs.getString("name"));
        LineString line = (LineString)getGeometry(rs);
        assertEquals(2, line.numPoints());

        rs.next();
        assertEquals("10", rs.getString("name"));
        lines = (MultiLineString)getGeometry(rs);
        assertEquals(3, lines.numGeoms());

        rs.next();
        assertEquals("11", rs.getString("name"));
        lines = (MultiLineString)getGeometry(rs);
        assertEquals(3, lines.numGeoms());
        //System.out.println(toGeoJson(lines));

        rs.close();
    }

// *** Dedupe complex objects (multiple tags etc)

    @Test
    public void dedupe_complex() throws SQLException {

// **** 1. Even merge
        Double importance = 0.9;
        Integer rank_search = null;
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        String name = name("1");
        Integer admin_level = null;
        String wikidata = wikidata("1");
        String geometry = linestring();
        String wikipedia = "1";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        name = name("1");
        wikidata = wikidata("2");
        wikipedia = "2";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

// **** 10. Uneven merge
        importance = 0.8;
        osm_id = 10;
        name = name("10");
        wikidata = wikidata("10");
        wikipedia = "10";
        class_ = "historic"; //low prio
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);
        class_ = "highway"; //high prio
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 11;
        name = name("10");
        wikidata = wikidata("11");
        wikipedia = "11";
        class_ = "natural"; //low prio
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);


// **** 100. Complex
        importance = 0.5;
        osm_id = 100;
        name = name("100");
        wikidata = wikidata("100");
        wikipedia = "100";
        class_ = "historic"; //low prio
        geometry = linestring(1,1,2,2);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = null;
        osm_id = 1010101010;
        name = name("101");
        class_ = "natural"; //low prio
        wikidata = null;
        wikipedia = "101";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);
        osm_id = 101;
        importance = 1.0;
        name = name("100");
        geometry = linestring(1,1,101,1);
        wikipedia = "101";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        importance = 0.1;
        osm_id = 102;
        wikipedia = null;
        name = name("100");
        class_ = "highway"; //high prio
        geometry = linestring(1,1,2,2);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);


// **** Assertions
        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(3, locaSize());

        ResultSet rs = locaElems();
        rs.next();
        assertEquals("100", rs.getString("name"));
        assertEquals("highway", rs.getString("subcat"));
        assertEquals("N101,N100,N102", rs.getString("osm_ids"));
        MultiLineString lines = (MultiLineString)getGeometry(rs);
        assertEquals(2, lines.numGeoms());
        assertEquals(4, lines.numPoints());
        assertTrue(lines.length() > 100);

        rs.next();
        assertEquals("1", rs.getString("name"));
        assertEquals("N1,N2", rs.getString("osm_ids"));
        LineString line = (LineString)getGeometry(rs);
        assertEquals(linestring(), line.toString());

        rs.next();
        assertEquals("10", rs.getString("name"));
        assertEquals("highway", rs.getString("subcat"));
        assertEquals("N10,N11", rs.getString("osm_ids"));
        line = (LineString)getGeometry(rs);
        assertEquals(linestring(), line.toString());

        rs.close();
    }

// *** Dedupe at different latitudes

    @Test
    public void dedupe_lat0() throws SQLException {
        double lat = 0;
        double Din = 99; //IN_MERGE_DISTANCE < MAX_DEDUPE_DISTANCE
        double Dout = 101; //OUT_MERGE_DISTANCE > MAX_DEDUPE_DISTANCE

        Double importance = 1.0;
        String name = name("north");
        long osm_id = 1;
        String geometry = linestring(-1, lat, 0, lat);
        char osm_type = 'W';
        String class_ = "place";
        String type = "suburb";
        Integer rank_search = null;
        String wikidata = null;
        Integer admin_level = null;
        String wikipedia = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        geometry = linestring(mToDegAtEquator(Din), lat, 1, lat);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 3;
        geometry = linestring(1+mToDegAtEquator(Dout), lat, 2, lat);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(2, locaSize());
    }

    @Test
    public void dedupe_lat30() throws SQLException {
        double lat = 30;
        double Din = 85; //IN_MERGE_DISTANCE < MAX_DEDUPE_DISTANCE
        double Dout = 90; //OUT_MERGE_DISTANCE > MAX_DEDUPE_DISTANCE

        Double importance = 1.0;
        String name = name("north");
        long osm_id = 1;
        String geometry = linestring(-1, lat, 0, lat);
        char osm_type = 'W';
        String class_ = "place";
        String type = "suburb";
        Integer rank_search = null;
        String wikidata = null;
        Integer admin_level = null;
        String wikipedia = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        geometry = linestring(mToDegAt30(Din), lat, 1, lat);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 3;
        geometry = linestring(1+mToDegAt30(Dout), lat, 2, lat);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(2, locaSize());
    }

    @Test
    public void dedupe_lat60() throws SQLException {
        double lat = 60;
        double Din = 50; //IN_MERGE_DISTANCE < MAX_DEDUPE_DISTANCE
        double Dout = 55; //OUT_MERGE_DISTANCE > MAX_DEDUPE_DISTANCE

        Double importance = 1.0;
        String name = name("north");
        long osm_id = 1;
        String geometry = linestring(-1, lat, 0, lat);
        char osm_type = 'W';
        String class_ = "place";
        String type = "suburb";
        Integer rank_search = null;
        String wikidata = null;
        Integer admin_level = null;
        String wikipedia = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        geometry = linestring(mToDegAt60(Din), lat, 1, lat);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 3;
        geometry = linestring(1+mToDegAt60(Dout), lat, 2, lat);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(2, locaSize());
    }

    @Test
    public void dedupe_lat85() throws SQLException {
        double lat = 85;
        double Din = 8; //IN_MERGE_DISTANCE < MAX_DEDUPE_DISTANCE
        double Dout = 10; //OUT_MERGE_DISTANCE > MAX_DEDUPE_DISTANCE

        Double importance = 1.0;
        String name = name("north");
        long osm_id = 1;
        String geometry = linestring(-1, lat, 0, lat);
        char osm_type = 'W';
        String class_ = "place";
        String type = "suburb";
        Integer rank_search = null;
        String wikidata = null;
        Integer admin_level = null;
        String wikipedia = null;
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 2;
        geometry = linestring(mToDegAt85(Din), lat, 1, lat);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        osm_id = 3;
        geometry = linestring(1+mToDegAt85(Dout), lat, 2, lat);
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);

        App.fillLocaDb(nomTestDb, locaTestDb);
        assertEquals(2, locaSize());
    }


// *** Dedupe geoms crossing 180th meridian

    @Test
    public void dedupe_cross180thMeridian() throws SQLException {

    }

// ** Test simplify geometries

    //@Test
    public void simplifyGeometries() throws SQLException {
        Double importance = null;
        Integer rank_search = null;
        long osm_id = 1;
        char osm_type = 'N';
        String class_ = "place";
        String type = "suburb";
        String name = name("1");
        String wikidata = null;
        Integer admin_level = null;
        String wikipedia = null;

        long place_id = 329195; //big polygon
        // long place_id = 306124; //long linestring
        // long placeid = 214330; //small linestring
        String geometry = geometryFromRealNomDb(329195).toString();
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);


    }



// * Utils

    private static int locaSize() throws SQLException {
        ResultSet rs = locaTestDb.executeQuery("select count(*) from elems");
        rs.next();
        int size = rs.getInt(1);
        rs.close();
        return size;
    }

    private static ResultSet locaElems() throws SQLException {
        return locaTestDb.executeQuery("select * from elems order by popindex");
    }

    /**
     * Insert specified elems in nom test db. Other column than specified by parameters set to NULL.
     * place_id in nom-test-db is ignored -> defaults to unique value (because of SERIAL).
     *
     * @param name hstore
     * @param wikidata extratags-hstore
     */
    private static void nomInsert(Double importance, Integer rank_search, long osm_id, char osm_type, String class_, String type, String name, Integer admin_level, String wikidata, String geometry, String wikipedia) throws SQLException {
        String sql = String.format("INSERT INTO placex (parent_place_id, linked_place_id, importance, indexed_date, geometry_sector, rank_address, rank_search, partition, indexed_status, osm_id, osm_type, class, type, name, admin_level, address, extratags, geometry, wikipedia, country_code, housenumber, postcode, centroid) VALUES (NULL, NULL, %s, NULL, NULL, NULL, %s, NULL, NULL, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s, NULL, NULL, NULL, NULL)", importance, rank_search, osm_id, quote(osm_type+""), quote(class_), quote(type), quote(name), admin_level, quote(wikidata), quote(geometry), quote(wikipedia));

        nomTestDb.executeUpdate(sql);
    }

    private static void nomInsert(Double importance, Integer rank_search, long osm_id, char osm_type, String class_, String type, String name, Integer admin_level) throws SQLException {
        String wikidata = null;
        String wikipedia = null;
        String geometry = point();
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, wikidata, geometry, wikipedia);
    }

    private static String quote(String str) {
        if (str == null) return "NULL";
        else return "'" + str + "'";
    }

    /**
     * @return name hstore with name.
     */
    public static String name(String name) {
        return String.format("\"name\"=>\"%s\"", name);
    }

    /**
     * @return extratags hstore with wikidata.
     */
    public static String wikidata(String wikidata) {
        return String.format("\"wikidata\"=>\"%s\"", wikidata);
    }

    private static String point(double lon, double lat) {
        return String.format("SRID=4326;POINT(%s %s)", lon, lat);
    }
    private static String point() {
        return "SRID=4326;POINT(1 1)";
    }

    private static String linestring(double lon1, double lat1, double lon2, double lat2) {
        return String.format("SRID=4326;LINESTRING(%s %s,%s %s)", lon1, lat1, lon2, lat2);
    }
    private static String linestring() {
        return "SRID=4326;LINESTRING(1 1,2 2)";
    }

    private static String polygon(double lon1, double lat1, double lon2, double lat2, double lon3, double lat3, double lon4, double lat4) {
        return String.format("SRID=4326;POLYGON((%s %s,%s %s,%s %s,%s %s,%s %s))", lon1, lat1, lon2, lat2, lon3, lat3, lon4, lat4, lon1, lat1);
    }
    private static String polygon() {
        return "SRID=4326;POLYGON((1 1,2 1,2 2,1 2,1 1))";
    }


    private static Geometry getGeometry(ResultSet rs) throws SQLException {
        PGgeometry pg = (PGgeometry)rs.getObject("geometry");
        return (pg == null ? null : pg.getGeometry());
    }

    private static Geometry geometryFromRealNomDb(long place_id) throws SQLException {
	String url = "jdbc:postgresql://localhost/nominatim";
	Connection conn = DriverManager.getConnection(url, "postgres", "pass");
	Statement st = conn.createStatement();
        String sql = String.format("select geometry from placex where place_id=%s", place_id);
        ResultSet rs = st.executeQuery(sql);
        rs.next();
        Geometry geom = getGeometry(rs);
        rs.close();
        st.close();
        conn.close();
	return geom;
    }

    private static double mToDegAtEquator(double m) {
        return m / 111319.458;
    }

    private static double mToDegAt85(double m) {
        return m / 9734.518271497967;
    }

    private static double mToDegAt30(double m) {
        return m / 96486.24755677342;
    }

    private static double mToDegAt60(double m) {
        return m / 55799.979000000014;
    }

    private static double mToDegAtLat(double m, double lat) {
        double a = 6378.137;
        double b = 6356.7523;
        lat = Math.toRadians(lat);
        //double R = Math.sqrt( (Math.pow(Math.pow(a, 2) * Math.cos(lat), 2) + Math.pow(Math.pow(b, 2) * Math.sin(lat), 2)) / (Math.pow(a * Math.cos(lat), 2) + Math.pow(b * Math.sin(lat), 2)) );
        double r = Math.pow(a*b, 2) / Math.pow( Math.pow(a*Math.cos(lat), 2) + Math.pow(b*Math.sin(lat), 2), 3.0/2);

        return r * 2 * Math.PI / 360;
    }

    private static String toGeoJson(String geom) throws SQLException {
        String sql = String.format("select ST_AsGeoJSON(GeomFromEWKT('%s'))", geom);
        //System.out.println(sql);
        ResultSet rs = nomTestDb.executeQuery(sql);
        rs.next();
        String geoJ = rs.getString(1);
        rs.close();
        return geoJ;
    }
    private static String toGeoJson(Geometry geom) throws SQLException {
        return toGeoJson(geom.toString());
    }

// * Before each test

    @Before
    public void resetTestDbs() throws SQLException {
        nomTestDb.executeUpdate("delete from placex");
        locaTestDb.executeUpdate("delete from elems");
    }

// * Before/after class

    @BeforeClass
    public static void createNomTestDb() throws SQLException {
	String url = "jdbc:postgresql://localhost/postgres";
	Connection conn = DriverManager.getConnection(url, "postgres", "pass");
	Statement st = conn.createStatement();
        st.execute("DROP DATABASE IF EXISTS nom_test");
	st.execute("CREATE DATABASE nom_test");
        st.close();
        conn.close();

	url = "jdbc:postgresql://localhost/nom_test";
	conn = DriverManager.getConnection(url, "postgres", "pass");
	nomTestDb = conn.createStatement();

        nomTestDb.execute("CREATE EXTENSION hstore");
        nomTestDb.execute("CREATE EXTENSION postgis");
	nomTestDb.executeUpdate("CREATE TABLE placex (place_id SERIAL, parent_place_id bigint, linked_place_id bigint, importance double precision, indexed_date timestamp without time zone, geometry_sector integer, rank_address smallint, rank_search smallint, partition smallint, indexed_status smallint, osm_id bigint NOT NULL, osm_type character(1) NOT NULL, class text NOT NULL, type text NOT NULL, name hstore, admin_level smallint, address hstore, extratags hstore, geometry geometry(Geometry,4326) NOT NULL, wikipedia text, country_code character varying(2), housenumber text, postcode text, centroid geometry(Geometry,4326))");
    }

    @BeforeClass
    public static void createLocaTestDb() throws SQLException {
	String url = "jdbc:postgresql://localhost/postgres";
	Connection conn = DriverManager.getConnection(url, "postgres", "pass");
	Statement st = conn.createStatement();
        st.execute("DROP DATABASE IF EXISTS loca_test");
	st.execute("CREATE DATABASE loca_test");
        st.close();
        conn.close();

        url = "jdbc:postgresql://localhost/loca_test";
	conn = DriverManager.getConnection(url, "postgres", "pass");
	locaTestDb = conn.createStatement();

        locaTestDb.execute("CREATE EXTENSION postgis");
	locaTestDb.executeUpdate("CREATE TABLE elems (" +
                         "popindex BIGINT PRIMARY KEY, " +
                         "name TEXT NOT NULL, " +
                         "supercat TEXT NOT NULL, " +
                         "subcat TEXT NOT NULL, " +
                         "geometry geometry(Geometry,4326) NOT NULL, " +
                         "area DOUBLE PRECISION NOT NULL, " +
                         "osm_ids TEXT NOT NULL)");
        locaTestDb.execute("CREATE INDEX ON elems USING GIST (geometry)");
        locaTestDb.execute("VACUUM ANALYZE");
    }

    //@AfterClass
    public static void removeTestDbs() throws SQLException {
        Connection nomConn = nomTestDb.getConnection();
        Connection locaConn = locaTestDb.getConnection();
        nomTestDb.close();
        locaTestDb.close();
        nomConn.close();
        locaConn.close();

	String url = "jdbc:postgresql://localhost/postgres";
	Connection conn = DriverManager.getConnection(url, "postgres", "pass");
	Statement st = conn.createStatement();
        st.execute("DROP DATABASE IF EXISTS nom_test");
        st.execute("DROP DATABASE IF EXISTS loca_test");
        st.close();
    }
}
