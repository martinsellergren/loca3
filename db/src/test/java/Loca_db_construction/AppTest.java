/*
 * Test the nom_query. Create the db with some elems, run the query, evaluate result, repeate.
 */
package Loca_db_construction;

import org.junit.*;
import static org.junit.Assert.*;

import java.sql.*;

public class AppTest {

    private static Statement nomTestingDb;
    private static String query;

    @Test
    public void emptyNomDb() throws SQLException {
        ResultSet rs = App.executeNomQuery(query, nomTestingDb);
        assertFalse(rs.next());
    }

    @Test
    public void singleElement() throws SQLException {
        String sql = "INSERT INTO placex (place_id, parent_place_id, linked_place_id, importance, indexed_date, geometry_sector, rank_address, rank_search, partition, indexed_status, osm_id, osm_type, class, type, name, admin_level, address, extratags, geometry, wikipedia, country_code, housenumber, postcode, centroid) VALUES (103698, 103697, NULL, 0, '2019-03-14 18:52:31.995466', 112483440, 20, 20, 112, 0, 27527229, 'N', 'place', 'suburb', '\"name\"=>\"Östermalm\"', 15, NULL, '\"wikidata\"=>\"Q29024431\", \"wikipedia\"=>\"sv:Östermalm, Västerås\"', '0101000020E6100000655C27E4398D3040016DAB5967CE4D40', 'sv:Östermalm,_Västerås', 'se', NULL, '722 14', '0101000020E6100000655C27E4398D3040016DAB5967CE4D40')";
        nomTestingDb.executeUpdate(sql);

        ResultSet rs = App.executeNomQuery(query, nomTestingDb);
        assertTrue(rs.next());
        assertFalse(rs.next());
    }

// * Before each test

    @Before
    public void resetTestDb() throws SQLException {
        nomTestingDb.executeUpdate("delete from placex");
    }

// * Before/after class
    @BeforeClass
    public static void createTestDb() throws SQLException {
	String url = "jdbc:postgresql://localhost/postgres";
	Connection conn = DriverManager.getConnection(url, "postgres", "pass");
	Statement st = conn.createStatement();
        st.execute("DROP DATABASE IF EXISTS nom_test");
	st.execute("CREATE DATABASE nom_test");
        st.close();
        conn.close();

	url = "jdbc:postgresql://localhost/nom_test";
	conn = DriverManager.getConnection(url, "postgres", "pass");
	nomTestingDb = conn.createStatement();

        nomTestingDb.execute("CREATE EXTENSION hstore");
        nomTestingDb.execute("CREATE EXTENSION postgis");
	nomTestingDb.executeUpdate("CREATE TABLE placex (place_id bigint NOT NULL, parent_place_id bigint, linked_place_id bigint, importance double precision, indexed_date timestamp without time zone, geometry_sector integer, rank_address smallint, rank_search smallint, partition smallint, indexed_status smallint, osm_id bigint NOT NULL, osm_type character(1) NOT NULL, class text NOT NULL, type text NOT NULL, name hstore, admin_level smallint, address hstore, extratags hstore, geometry geometry(Geometry,4326) NOT NULL, wikipedia text, country_code character varying(2), housenumber text, postcode text, centroid geometry(Geometry,4326))");
    }

    @BeforeClass
    public static void readNomQuery() {
        query = App.readNomQueryFromFile();
    }

    @AfterClass
    public static void removeTestDb() throws SQLException {
        Connection conn = nomTestingDb.getConnection();
        nomTestingDb.close();
        conn.close();

	String url = "jdbc:postgresql://localhost/postgres";
	conn = DriverManager.getConnection(url, "postgres", "pass");
	Statement st = conn.createStatement();
        st.execute("DROP DATABASE IF EXISTS nom_test");
        st.close();
    }
}
