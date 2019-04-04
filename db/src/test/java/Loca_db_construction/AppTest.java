/*
 * Test the nom_query. Create the db with some elems, run the query, evaluate result, repeate.
 */
package Loca_db_construction;

import org.junit.*;
import static org.junit.Assert.*;

import java.sql.*;
import java.util.List;
import java.util.ArrayList;

public class AppTest {

    private static Statement nomTestDb;
    private static Statement locaTestDb;

// * Tests

    @Test
    public void emptyNomDb() throws SQLException {
        App.fillLocaDb(nomTestDb, locaTestDb);
        ResultSet rs = locaTestDb.executeQuery("select count(*) from elems");
        rs.next();
        int size = rs.getInt(1);
        assertEquals(0, size);
    }

    @Test
    public void singleElement() throws SQLException {
        nomInsert(103698, 0, 20, 27527229, 'N', "place", "suburb", "Östermalm", 15, "Q29024431", "sv:Östermalm, Västerås", null, "sv:Östermalm,_Västerås");
        App.fillLocaDb(nomTestDb, locaTestDb);

        ResultSet rs = locaTestDb.executeQuery("select * from elems");
        rs.next();
        assertEquals("Östermalm", rs.getString("name"));
        assertEquals(1, locaSize());
    }

// * Utils

    private static int locaSize() throws SQLException {
        ResultSet rs = locaTestDb.executeQuery("select count(*) from elems");
        rs.next();
        return rs.getInt(1);
    }


    /**
     * @param osm_type N/W/R
     */
    private static void nomInsert(int place_id, double importance, double rank_search, long osm_id, char osm_type, String class_, String type, String name, int admin_level, String extratags_wikidata, String extratags_wikipedia, List<double[]> geometry, String wikipedia) throws SQLException {
        String fakeGeometry = "'0101000020E6100000655C27E4398D3040016DAB5967CE4D40'";
        String sql = String.format("INSERT INTO placex (place_id, parent_place_id, linked_place_id, importance, indexed_date, geometry_sector, rank_address, rank_search, partition, indexed_status, osm_id, osm_type, class, type, name, admin_level, address, extratags, geometry, wikipedia, country_code, housenumber, postcode, centroid) VALUES (%s, NULL, NULL, %s, NULL, NULL, NULL, %s, NULL, NULL, %s, '%s', '%s', '%s', '\"name\"=>\"%s\"', %s, NULL, '\"wikidata\"=>\"%s\", \"wikipedia\"=>\"%s\"', %s, '%s', NULL, NULL, NULL, NULL)", place_id, importance, rank_search, osm_id, osm_type, class_, type, name, admin_level, extratags_wikidata, extratags_wikipedia, fakeGeometry, wikipedia);

// VALUES (103698, 103697, NULL, 0, '2019-03-14 18:52:31.995466', 112483440, 20, 20, 112, 0, 27527229, 'N', 'place', 'suburb', '\"name\"=>\"Östermalm\"', 15, NULL, '\"wikidata\"=>\"Q29024431\", \"wikipedia\"=>\"sv:Östermalm, Västerås\"', '0101000020E6100000655C27E4398D3040016DAB5967CE4D40', 'sv:Östermalm,_Västerås', 'se', NULL, '722 14', '0101000020E6100000655C27E4398D3040016DAB5967CE4D40')");
        nomTestDb.executeUpdate(sql);
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
	nomTestDb.executeUpdate("CREATE TABLE placex (place_id bigint NOT NULL, parent_place_id bigint, linked_place_id bigint, importance double precision, indexed_date timestamp without time zone, geometry_sector integer, rank_address smallint, rank_search smallint, partition smallint, indexed_status smallint, osm_id bigint NOT NULL, osm_type character(1) NOT NULL, class text NOT NULL, type text NOT NULL, name hstore, admin_level smallint, address hstore, extratags hstore, geometry geometry(Geometry,4326) NOT NULL, wikipedia text, country_code character varying(2), housenumber text, postcode text, centroid geometry(Geometry,4326))");
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

    @AfterClass
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
