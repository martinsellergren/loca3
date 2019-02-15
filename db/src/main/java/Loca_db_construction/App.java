/*
 * For creating the loca db. Source is an existing nominatim db.
 *
 * Post: Loca db with geo-objects.
 * - Geo-objects have unique osm-id.
 * - May have same names + categories.
 */
package Loca_db_construction;

import java.sql.*;
import java.util.List;
import java.util.ArrayList;
import org.postgis.*;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App {

    // 0en 1sv 2sp
    public static final int LANGUAGE = 0;

    private static Map<String,String[]> keyConversionTable;
    private static Map<String,String[]> keyValueConversionTable;

    static {
        loadKeyConversionTable();
        loadKeyValueConversionTable();
    }

    public static void main(String[] args) throws SQLException {
	Statement nomDb = connectToNomDb();
	//testGeom(nomDb, "placex");

	Statement locaDb = createLocaDb();
	fillLocaDb(nomDb, locaDb);
	//dedupeRoads(locaDb);
	//testGeom(locaDb, "elems");

	// List<double[]> area = new ArrayList<>();
	// area.add(new double[]{-12.4993515, -37.4106643});
	// area.add(new double[]{-12.5110245, -37.4398378});
	// area.add(new double[]{-12.4567795, -37.4376570});
	// area.add(new double[]{-12.4545479, -37.3966189});
	// area.add(new double[]{-12.4993515, -37.4106643});
	// long popindex = 100;

	// queryLocaDb(locaDb, area, popindex);

	locaDb.close();
	nomDb.close();
    }

    /**
     * Process data in nominatim-db and insert into loca-db.
     * Nom-db may contain multile elements for same osm-object:
     * - For popularity: use pop of entry with max pop.
     * - For category: Pick entry according to key-conversion-table.
     * Insert:
     * - popindex: increasing with popularity
     * - super- and sub-category from conversion-tables (based on tagging)
     */
    private static void fillLocaDb(Statement nomDb, Statement locaDb) throws SQLException {
	locaDb.executeUpdate("CREATE TABLE elems (" +
			     "popindex BIGINT PRIMARY KEY, " +
			     "name TEXT NOT NULL, " +
			     "supercat TEXT NOT NULL, " +
			     "subcat TEXT NOT NULL, " +
			     "geometry geometry(Geometry,4326) NOT NULL, " +
			     "osm_id TEXT UNIQUE NOT NULL)");

        String query = readNomQueryFromFile();
        ResultSet rs = nomDb.executeQuery(query);
        long popindex = 0;

        while (rs.next()) {
            String name = fixName(rs.getString(1));
            if (!okName(name)) continue;
            String[] tagKeys = rs.getString(2).split("_:_");
            String[] tagValues = rs.getString(3).split("_:_");
            int adminLevel = rs.getInt(4);
            PGgeometry shape = (PGgeometry)rs.getObject(5);
            String osmId = rs.getString(6) + ":" + rs.getString(7);

            String[] tag = pickTag(tagKeys, tagValues);
            String[] cats = tagToSuperAndSubCategory(tag[0], tag[1], adminLevel);
            String supercat = cats[0];
            String subcat = cats[1];

	    String sql = String.format("INSERT INTO elems VALUES " +
				       "(%s, $$%s$$, '%s', '%s', '%s', '%s') " +
				       "ON CONFLICT (osm_id) DO NOTHING",
				       popindex++, name, supercat, subcat, shape, osmId);
	    locaDb.executeUpdate(sql);

            //System.out.format("%s %s %s %s\n", name, tagKey, tagValue, webAddress(osmId));
        }

	//ResultSet rs = nomDb.executeQuery("SELECT class,type,name,geometry FROM placex ORDER BY importance");
	// ResultSet rs = nomDb.executeQuery("SELECT name,class,type,geometry,osm_type,osm_id FROM placex ORDER BY rank_search");
        // List<String> tagsSpec = loadTagsSpec();
	// long popindex = 0;

	// while (rs.next()) {
	//     String name = rs.getString(1);
	//     if (!okName(name)) continue;
	//     String subcat = subcat(rs.getString(2), rs.getString(3), tagsSpec);
	//     String supercat = supercat(subcat);
	//     PGgeometry shape = (PGgeometry)rs.getObject(4);
	//     String osmId = rs.getString(5) + ":" + rs.getString(6);

	//     String sql = String.format("INSERT INTO elems VALUES " +
	// 			       "(%s, $$%s$$, '%s', '%s', '%s', '%s') " +
	// 			       "ON CONFLICT (osm_id) DO NOTHING",
	// 			       popindex++, name, supercat, subcat, shape, osmId);
	//     locaDb.executeUpdate(sql);
	// }
	rs.close();
    }

    /**
     * Pick tag based on order in key-conversion-table.
     */
    private static String[] pickTag(String keys[], String value[]) {

        return null;
    }

    /**
     * Converts tag to super and sub-category. Conversion based on
     * conversion tables.
     *
     * If key=boundary and value=administrative: adminLevel decides cat.
     * @return [supercat subcat]
     */
    private static String[] tagToSuperAndSubCategory(String key, String value, int adminLevel) {
        if (key.equals("boundary") && value.equals("administrative"))
            return adminBoundaryRankToCategory(adminLevel);


    }

    /**
     * Convert nominatim's admin-level to category for admin-boundaries.
     */
    private static String[] adminBoundaryRankToCategory(int adminLevel) {
        return null;
    }

    /**
     * @return File-text.
     */
    private static List<String> readFile(String path) {
        try {
            Charset charset = Charset.forName("UTF-8");
            return Files.readAllLines(Paths.get(path), charset);
        }
        catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
            return null;
        }
    }

    /**
     * @return Query for extracting data from nom-db.
     */
    private static String readNomQueryFromFile() {
        List<String> xs = readFile("/home/martin/loca3/loca3/nom_query.sql");

        StringBuilder sb = new StringBuilder();
        for (String x : xs) {
            sb.append(x + " ");
        }

        return sb.toString();
    }

    /**
     * @return True if name ok, i.e not emtpy etc.
     */
    private static boolean okName(String name) {
        //TODO?
	return name != null && name.length() > 1;
    }

    /**
     * @return Name fixed, e.g decapitalize if necessary.
     */
    private static String fixName(String name) {
        //TODO
        return name;
    }

    private static void dedupeRoads(Statement locaDb) {
	// TODO
    }

    private static String subcat(String key, String value, List<String> tagsSpec) {
	//return (value.equalsIgnoreCase("yes") ? key : value);

        for (String line : tagsSpec) {
            //System.out.println(line);
            String[] parts = line.split("\\|");
            String key2 = parts[0];
            String value2 = parts[1];
            String subcat = parts[2 + LANGUAGE];

            if ((key.equalsIgnoreCase(key2) && value.equalsIgnoreCase(value2)) ||
                (key.equalsIgnoreCase(key2) && value2.equals("-"))) {

                //System.out.printf("%s, %s, %s\n", key, value, subcat);
                return subcat;
            }
        }

        System.out.printf("******UNSPECIFIED: %s, %s\n", key, value);
	return "UNSPECIFIED";
    }

    private static String supercat(String subcat) {
	return "Civilization";
    }

    /**
     * @param area Return elements in this area.
     * @param popindexLimit Return elements with popindex larger than this.
     * @param count Number of elements to return.
     *
     * @return Geo-objects inside area, [lon lat]
     */
    public static List<GeoObject> queryLocaDb(Statement st, List<double[]> area, long popindexLimit) throws SQLException {
	String sql = String.format("SELECT * FROM elems WHERE ST_Intersects(geometry, 'SRID=4326;%s') AND popindex > %s",
				   polystr(area), popindexLimit);
	ResultSet rs = st.executeQuery(sql);

	while (rs.next()) {
	    long popindex = rs.getLong(1);
	    String name = rs.getString(2);
	    String supercat = rs.getString(3);
	    String subcat = rs.getString(4);
	    PGgeometry shape = (PGgeometry)rs.getObject(5);
	    String osmId = rs.getString(6);

	    System.out.printf("%s. %s. %s. %s. %s\n",
			      popindex, defaultName(name), supercat, subcat, webAddress(osmId));
	}

	return null;
    }

    /**
     * @param ns [lon lat]
     * @return "POLYGON((lon lat,lon lat,lon lat,...))"
     */
    private static String polystr(List<double[]> ns) {
	StringBuilder sb = new StringBuilder();

	for (double[] n : ns) {
	    sb.append(String.format("%s %s,", n[0], n[1]));
	}

	String str = sb.toString();
	return "POLYGON((" + str.substring(0, str.length()-1) + "))";
    }

    /**
     * @param name Name-data, multiple names from different countries.
     */
    private static String defaultName(String name) {
	String regex = "\"name\"=>\"(.*?)\"";
	Pattern p = Pattern.compile(regex);
	Matcher m = p.matcher(name);

	if (!m.find()) throw new RuntimeException("No default name:\n" + name);
	return m.group(1);
    }

    private static String webAddress(String osmId) {
	String type = osmId.split(":")[0];
	String id = osmId.split(":")[1];
	type = type.equals("N") ? "node" : (type.equals("W") ? "way" : "relation");
	return String.format("https://www.openstreetmap.org/%s/%s", type, id);
    }

    private static Statement createLocaDb() throws SQLException {
	String url = "jdbc:postgresql://localhost/";
	Connection conn = DriverManager.getConnection(url + "nominatim", "martin", "pass");
	Statement st = conn.createStatement();
        st.execute("DROP DATABASE IF EXISTS loca");
	st.execute("CREATE DATABASE loca");
        st.close();
        conn.close();

	conn = DriverManager.getConnection(url + "loca", "martin", "pass");
	//((org.postgresql.PGConnection)conn).addDataType("geometry", "org.postgis.PGgeometry");
	st = conn.createStatement();
	st.execute("CREATE EXTENSION postgis");
	return st;
    }

    private static Statement connectToNomDb() throws SQLException {
	String url = "jdbc:postgresql://localhost/nominatim";
	Connection conn = DriverManager.getConnection(url, "martin", "pass");

	conn.setAutoCommit(false);
	Statement st = conn.createStatement();
	st.setFetchSize(50);
	return st;
    }

    private static void testGeom(Statement st, String table) throws SQLException {
	ResultSet r = st.executeQuery("SELECT geometry,name FROM " + table);

	while(r.next()) {
	    /*
	     * Retrieve the geometry as an object then cast it to the geometry type.
	     * Print things out.
	     */
	    PGgeometry geom = (PGgeometry)r.getObject(1);
	    String name = r.getString(2);
	    System.out.println(name + ". " + geom);
	}
	st.close();
    }

    /**
     * Load conversion tables.
     */
    private static void loadKeyConversionTable() {
        //TODO
    }
    private static void loadKeyValueConversionTable() {

    }
}
