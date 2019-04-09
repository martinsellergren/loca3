/*
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
        String extratags = extratags("Q29024431", "sv:Östermalm, Västerås");
        String geometry = anyGeometry();
        String wikipedia = "sv:Östermalm,_Västerås";
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, extratags, geometry, wikipedia);
        App.fillLocaDb(nomTestDb, locaTestDb);

        ResultSet rs = locaElems();
        rs.next();
        assertEquals(1, rs.getLong("popindex"));
        assertEquals("Östermalm", rs.getString("name"));
        assertEquals("c", rs.getString("supercat"));
        assertEquals("place", rs.getString("subcat"));
        assertNotNull(getGeometry(rs));
        assertEquals(0, rs.getDouble("area"), 0.00001);
        assertEquals("node/27527229", rs.getString("osm_ids"));
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

    @Test public void locaOrder() throws SQLException {
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
     * @param extratags hstore
     */
    private static void nomInsert(Double importance, Integer rank_search, long osm_id, char osm_type, String class_, String type, String name, Integer admin_level, String extratags, String geometry, String wikipedia) throws SQLException {
        String sql = String.format("INSERT INTO placex (parent_place_id, linked_place_id, importance, indexed_date, geometry_sector, rank_address, rank_search, partition, indexed_status, osm_id, osm_type, class, type, name, admin_level, address, extratags, geometry, wikipedia, country_code, housenumber, postcode, centroid) VALUES (NULL, NULL, %s, NULL, NULL, NULL, %s, NULL, NULL, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s, NULL, NULL, NULL, NULL)", importance, rank_search, osm_id, quote(osm_type+""), quote(class_), quote(type), quote(name), admin_level, quote(extratags), quote(geometry), quote(wikipedia));

// VALUES (103698, 103697, NULL, 0, '2019-03-14 18:52:31.995466', 112483440, 20, 20, 112, 0, 27527229, 'N', 'place', 'suburb', '\"name\"=>\"Östermalm\"', 15, NULL, '\"wikidata\"=>\"Q29024431\", \"wikipedia\"=>\"sv:Östermalm, Västerås\"', '0101000020E6100000655C27E4398D3040016DAB5967CE4D40', 'sv:Östermalm,_Västerås', 'se', NULL, '722 14', '0101000020E6100000655C27E4398D3040016DAB5967CE4D40')");
        nomTestDb.executeUpdate(sql);
    }

    private static void nomInsert(Double importance, Integer rank_search, long osm_id, char osm_type, String class_, String type, String name, Integer admin_level) throws SQLException {
        String extratags = null;
        String wikipedia = null;
        String geometry = anyGeometry();
        nomInsert(importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, extratags, geometry, wikipedia);
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
     * @return extratags hstore with wikidata and wikipedia entries.
     */
    public static String extratags(String wikidata, String wikipedia) {
        return String.format("\"wikidata\"=>\"%s\", \"wikipedia\"=>\"%s\"", wikidata, wikipedia);
    }

    private static String anyGeometry() {
        return "SRID=4326;POINT(0 0)";
    }

    private static PGgeometry getGeometry(ResultSet rs) throws SQLException {
        return (PGgeometry)rs.getObject("geometry");
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
