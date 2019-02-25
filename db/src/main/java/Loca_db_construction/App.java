/*
 * For creating the loca db. Source is an existing nominatim db.
 *
 * Post: Loca db with geo-objects.
 * - Geo-objects have popindex, increasing with popularity.
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
import java.util.LinkedHashMap;
import java.util.Arrays;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App {

    // 0en 1sv 2sp
    public static final int LANGUAGE = 0;

    /**
     * key -> [super-cat(c/n/r), sub-cat] */
    private static Map<String,String[]> keyConversionTable;

    /**
     * key:value -> [super-cat(c/n/r), sub-cat]  */
    private static Map<String,String[]> keyValueConversionTable;

    static {
        keyConversionTable = loadKeyConversionTable();
        keyValueConversionTable = loadKeyValueConversionTable(keyConversionTable);
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
            String[] tagKeys = (String[])rs.getArray(2).getArray();
            String[] tagValues = (String[])rs.getArray(3).getArray();
            Short[] adminLevels = (Short[])rs.getArray(4).getArray();
            PGgeometry[] shapes = toShapes(rs.getArray(5).getArray());
            String osmId = rs.getString(6) + ":" + rs.getString(7);

            String[] tag = pickTag(tagKeys, tagValues, adminLevels);
            String[] cats = tagToSuperAndSubCategory(tag[0], tag[1], Integer.parseInt(tag[2]));
            String supercat = cats[0];
            String subcat = cats[1];
            //System.out.format("%s:%s:%s -> %s:%s\n", tag[0], tag[1], tag[2], supercat, subcat);
            PGgeometry shape = pickShape(shapes);

	    String sql = String.format("INSERT INTO elems VALUES " +
	        		       "(%s, $$%s$$, '%s', '%s', '%s', '%s') " +
	        		       "ON CONFLICT (osm_id) DO UPDATE SET name = '!!!!!CONFLICT!!!!!'",
	        		       popindex++, name, supercat, subcat, shape, osmId);
	    locaDb.executeUpdate(sql);
            System.out.format("%s %s %s %s\n", name, supercat, subcat, webAddress(osmId));
        }
	rs.close();
    }

    /**
     * @return List of shapes.
     */
    private static PGgeometry[] toShapes(Object masterObj) {
        Object objs[] = (Object[])masterObj;
        PGgeometry[] shapes = new PGgeometry[objs.length];
        for (int i = 0; i < objs.length; i++)
            shapes[i] = (PGgeometry)objs[i];
        return shapes;
    }

    /**
     * Pick tag based on order in key-conversion-table.
     * @return [key, value, adminLevel]. Admin-level only of interest
     * for specific tags.
     */
    private static String[] pickTag(String keys[], String values[], Short[] adminLevels) {
        int i = 0;
        for (Map.Entry<String, String[]> entry : keyConversionTable.entrySet()) {
            String tableKey = entry.getKey();
            int index = Arrays.asList(keys).indexOf(tableKey);
            if (index != -1) {
                return new String[]{keys[index], values[index], adminLevels[index]+""};
            }
        }
        throw new RuntimeException("Dead end");
    }

    /**
     * Converts tag to super and sub-category. Conversion based on
     * conversion tables.
     *
     * If key=boundary and value=administrative: adminLevel decides cat.
     * @return [supercat(c/r/n) subcat]
     */
    private static String[] tagToSuperAndSubCategory(String key, String value, int adminLevel) {
        if (key.equals("boundary") && value.equals("administrative"))
            return adminBoundaryRankToCategory(adminLevel);

        String[] cats = keyValueConversionTable.get(key + ":" + value);
        if (cats != null)
            return cats;
        else
            return keyConversionTable.get(key);
    }

    /**
     * Convert nominatim's admin-level to category for admin-boundaries.
     */
    private static String[] adminBoundaryRankToCategory(int adminLevel) {
        String subcat = "border";
        switch (adminLevel) {
        case 1: subcat = "continent"; break;
        case 2: subcat = "country"; break;
        case 3: subcat = "state"; break;
        case 4: subcat = "state"; break;
        case 5: subcat = "state district"; break;
        case 6: subcat = "county"; break;
        case 7: subcat = "county"; break;
        case 8: subcat = "city"; break;
        case 9: subcat = "city district";
        }
        return new String[]{"c", subcat};
    }

    /**
     * @param shapes Array of different representations of a certain
     * geo-object, ordered by 'in some sence' importance.
     * @return Shape picked to represent the object.
     */
    private static PGgeometry pickShape(PGgeometry[] shapes) {
        //TODO smart pick.
        return shapes[0];
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
			      popindex, name, supercat, subcat, webAddress(osmId));
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
     * @return Web address to osm-element.
     */
    private static String webAddress(String osmId) {
	String type = osmId.split(":")[0];
	String id = osmId.split(":")[1];
	type = type.equals("N") ? "node" : (type.equals("W") ? "way" : "relation");
	return String.format("https://www.openstreetmap.org/%s/%s", type, id);
    }

    /**
     * Create a new postgis db named loca. Remove if exists.
     */
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

    /**
     * Connect to existing nominatim db.
     * @return Handle to manipulate db.
     */
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
     * Load key-conversion-table into an ORDERED map.
     */
    private static Map<String, String[]> loadKeyConversionTable() {
        Map<String, String[]> table = new LinkedHashMap<>();
        List<String> entries = readFile("/home/martin/loca3/loca3/tag-to-category-conversion/key-conversion-table");
        entries = entries.subList(1, entries.size());

        for (String entry : entries) {
            String[] blocks = entry.split(" ");

            String key = blocks[0];
            String supercat = blocks[1];
            String subcat = blocks.length == 2 ?
                key :
                entry.replaceFirst(".*'(.+)'.*", "$1");

            table.put(key, new String[]{supercat, subcat});
        }
        return table;
    }

    /**
     * Load key-value-conversion-table into a regular map.
     */
    private static Map<String, String[]> loadKeyValueConversionTable(Map<String, String[]> keyConvTable) {
        Map<String, String[]> table = new HashMap<>();
        List<String> entries = readFile("/home/martin/loca3/loca3/tag-to-category-conversion/key-value-conversion-table");
        entries = entries.subList(1, entries.size());

        for (String entry :  entries) {
            String[] blocks = entry.split(" ");

            String key = blocks[0];
            String value = blocks[1];
            String supercat = blocks[2];
            String subcat = blocks.length == 3 ?
                keyConvTable.get(key)[1] :
                entry.replaceFirst(".*'(.+)'.*", "$1");

            table.put(key + ":" + value, new String[]{supercat, subcat});
        }
        return table;
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
        for (String x : xs) sb.append(x + " ");
        return sb.toString();
    }
}
