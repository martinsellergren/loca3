/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package Loca_db_construction;

import java.sql.*;
import java.util.List;
import java.util.ArrayList;

public class App {

    public String getGreeting() {
        return "Hello world.";
    }

    public static void main(String[] args) throws SQLException {
        System.out.println(new App().getGreeting());

	Statement nomDb = connectToNomDb();	
	Statement locaDb = createLocaDb();
	transferGeoElements(nomDb, locaDb);
      
	List<double[]> area = new ArrayList<>();
	area.add(new double[]{0, 0});
	area.add(new double[]{10, 0});
	area.add(new double[]{10, 10});
	//area.add(new double[]{0, 0});
	long popindex = 0;
	
	queryLocaDb(locaDb, area, popindex);

	nomDb.close();
	locaDb.close();
    }

    /**
     * Insert: popindex(increasing with popularity), name, super-category(Nature/Civilization/Road), sub-category, shape
     */
    private static void transferGeoElements(Statement nomDb, Statement locaDb) throws SQLException {
	locaDb.executeUpdate("CREATE TABLE elems (" +
			     "popindex BIGINT not null, " +
			     "name TEXT not null, " +
			     "supercat TEXT not null, " +
			     "subcat TEXT not null, " +
			     "shape TEXT not null, " +
			     "PRIMARY KEY ( popindex ))");
	
	//ResultSet rs = nomDb.executeQuery("SELECT class,type,name,geometry FROM placex ORDER BY importance");
	ResultSet rs = nomDb.executeQuery("SELECT name,class,type,geometry FROM placex ORDER BY rank_search");

	for (long popindex = 0; rs.next(); popindex++) {
	    String name = "name...";//rs.getString(1);
	    String subcat = subcat(rs.getString(2), rs.getString(3));
	    String supercat = supercat(subcat);
	    String shape = "shape...";//rs.getString(4);
 
	    String sql = String.format("INSERT INTO elems VALUES (%s, '%s', '%s', '%s', '%s')", popindex, name, supercat, subcat, shape);
	    locaDb.executeUpdate(sql);
	}
	rs.close();
    }

    private static String subcat(String key, String value) {
	//return (value.equalsIgnoreCase("yes") ? key : value);
	return key + " : " + value;
    }

    private static String supercat(String subcat) {
	return "Civilization";
    }

    /**
     * @param area Return elements in this area.
     * @param popindex Return elements with popindex larger than this.
     * @param count Number of elements to return.
     *
     * @return Geo-objects inside area.
     */
    public static List<GeoObject> queryLocaDb(Statement st, List<double[]> area, long popindex) throws SQLException {
	
	return null;
    }

    private static Statement createLocaDb() throws SQLException {
	String url = "jdbc:postgresql://localhost/";
	Connection conn = DriverManager.getConnection(url, "martin", "pass");
	Statement st = conn.createStatement();
	
	try {
	    st.executeUpdate("DROP DATABASE loca");
	} catch (Exception e) {}
	st.executeUpdate("CREATE DATABASE loca");

	conn = DriverManager.getConnection(url + "loca", "martin", "pass");
	return conn.createStatement();
    }

    private static Statement connectToNomDb() throws SQLException {
	String url = "jdbc:postgresql://localhost/nominatim";
	Connection conn = DriverManager.getConnection(url, "martin", "pass");
	conn.setAutoCommit(false);
	Statement st = conn.createStatement();
	st.setFetchSize(50);
	return st;	
    }
}