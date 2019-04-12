// * Head
/*
 * For creating the loca db. Source is an existing nominatim db.
 *
 * Pre:
 * Nom-db, owned by 'postgres', pass='pass'.
 * Nom-db  constraints:
 * - 0 <= place_id UNIQUE, NOT NULL
 * - 0 <= importance <= 1
 * - 2 <= rank_search <= 30
 * - 0 < osm_id, NOT NULL
 * - osm_type <- N/W/R, NOT NULL
 * - class_ One of those defined in key-conversion-table, NOT NULL
 * - type NOT NULL
 * - 0 <= admin_level <= 15
 * - geometry Postgis node/linestring/polygon/multi- (i.e any except geometrycollection), NOT NULL
 *
 * Post:
 * Loca db with geo-objects.
 * - Geo-objects have popindex, increasing with popularity.
 * - May have same names + categories.
 * - Elems in nom-db with same osm_id&type, wikidata, wikipedia are merged into one elem.
 * - Proximate (see MAX_DEDUPE_DISTANCE in nom-query) linestring-elems with same name merged into one.
 */
package Loca_db_construction;

// * Imports
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

// * Declarations

public class App {

    // 0en 1sv 2sp. TODO
    public static final int LANGUAGE = 0;

    /**
     * this * {row-content-size} ~= ram-usage. Used by nom-db query. */
    private static final int DB_FETCH_SIZE = 1000;

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

// * Main

    public static void main(String[] args) throws SQLException {
	Statement nomDb = connectToNomDb();
	Statement locaDb = createLocaDb();
	fillLocaDb(nomDb, locaDb);
        //Statement locaDb = connectToLocaDb();

	List<double[]> area = new ArrayList<>();
        area.add(new double[]{6.4071253, 0.4696603});
        area.add(new double[]{6.2251096, -0.1675413});
        area.add(new double[]{6.7856924, -0.0137329});
        area.add(new double[]{7.0137726, 0.4669138});
        area.add(new double[]{6.4071253, 0.4696603});

        // area.add(new double[]{6.4071253, 0.4696603});
        // area.add(new double[]{6.3061935, 0.0521851});
        // area.add(new double[]{6.7856924, -0.0137329});
        // area.add(new double[]{7.0137726, 0.4669138});
        // area.add(new double[]{6.4071253, 0.4696603});
	long popindex = 0;
	queryLocaDb(locaDb, area, popindex);

	nomDb.close();
	locaDb.close();
    }

// * Fill loca

    /**
     * Process data in nominatim-db and insert into loca-db.
     * Nom-db may contain multile elements for same osm-object:
     * - For popularity: use pop of entry with max pop.
     * - For category: Pick entry according to key-conversion-table.
     * Insert:
     * - popindex: increasing with popularity
     * - super- and sub-category from conversion-tables (based on tagging)
     */
    public static void fillLocaDb(Statement nomDb, Statement locaDb) throws SQLException {
        String query = readNomQueryFromFile();
        ResultSet rs = executeNomQuery(query, nomDb);

        while (rs.next()) {
            long popindex = rs.getLong(1);
            String name = rs.getString(2);
            String[] tagKeys = rs.getString(3).split(",");
            String[] tagValues = rs.getString(4).split(",");
            String[] adminLevels = rs.getString(5).split(",");

            PGgeometry shape = (PGgeometry)rs.getObject(6);
            String osmIds = rs.getString(7);

            String[] tag = pickTag(tagKeys, tagValues, adminLevels);
            String[] cats = tagToSuperAndSubCategory(tag[0], tag[1], Integer.parseInt(tag[2]));
            String supercat = cats[0];
            String subcat = cats[1];
            //System.out.format(".%s.%s = %s:%s:%s -> %s:%s\n", popindex, name, tag[0], tag[1], tag[2], supercat, subcat);

	    String sql = String.format("INSERT INTO elems VALUES " +
	        		       "(%s, $$%s$$, '%s', '%s', $$%s$$, ST_AREA('%s'), '%s')",
	        		       popindex, name, supercat, subcat, shape, shape, osmIds);
	    locaDb.executeUpdate(sql);
        }
	rs.close();
    }

    /**
     * Pick tag based on order in key-conversion-table.
     * @return [key, value, adminLevel]. Admin-level only of interest
     * for specific tags.
     */
    private static String[] pickTag(String keys[], String values[], String[] adminLevels) {
        int i = 0;
        for (Map.Entry<String, String[]> entry : keyConversionTable.entrySet()) {
            String tableKey = entry.getKey();
            int index = Arrays.asList(keys).indexOf(tableKey);
            if (index != -1) {
                return new String[]{keys[index], values[index], adminLevels[index]};
            }
        }
        throw new RuntimeException(String.format("Not defined in key-conversion-table: %s : %s : %s",
                                                 Arrays.toString(keys),
                                                 Arrays.toString(values),
                                                 Arrays.toString(adminLevels)));
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
        if (cats == null) cats = keyConversionTable.get(key);

        cats[1] = cats[1].replace('_', ' ');
        return cats;
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
     * @return Query for extracting data from nom-db.
     */
    private static String readNomQueryFromFile() {
        List<String> xs = readFile("../nom_query.sql");
        StringBuilder sb = new StringBuilder();
        for (String x : xs) sb.append(x + "\n");
        return sb.toString();
    }

// * Create/ connect to db

    /**
     * Create a new postgis db named loca. Remove if exists.
     * @return Handle to manipulate db.
     */
    private static Statement createLocaDb() throws SQLException {
	String url = "jdbc:postgresql://localhost/nominatim";
	Connection conn = DriverManager.getConnection(url, "postgres", "pass");
	Statement st = conn.createStatement();
        st.execute("DROP DATABASE IF EXISTS loca");
	st.execute("CREATE DATABASE loca");
        st.close();
        conn.close();

        st = connectToLocaDb();
        st.execute("CREATE EXTENSION IF NOT EXISTS postgis");
	st.executeUpdate("CREATE TABLE elems (" +
                         "popindex BIGINT PRIMARY KEY, " +
                         "name TEXT NOT NULL, " +
                         "supercat TEXT NOT NULL, " +
                         "subcat TEXT NOT NULL, " +
                         "geometry geometry(Geometry,4326) NOT NULL, " +
                         "area DOUBLE PRECISION NOT NULL, " +
                         "osm_ids TEXT NOT NULL)");
        st.execute("CREATE INDEX ON elems USING GIST (geometry)");
        st.execute("VACUUM ANALYZE");
        return st;
    }

    /**
     * Connect to existing loca db.
     * @return Handle to manipulate db.
     */
    private static Statement connectToLocaDb() throws SQLException {
        String url = "jdbc:postgresql://localhost/loca";
	Connection conn = DriverManager.getConnection(url, "postgres", "pass");
	return conn.createStatement();
    }

    /**
     * Connect to existing nominatim db.
     * @return Handle to manipulate db.
     */
    private static Statement connectToNomDb() throws SQLException {
	String url = "jdbc:postgresql://localhost/nominatim";
	Connection conn = DriverManager.getConnection(url, "postgres", "pass");
	Statement st = conn.createStatement();
	conn.setAutoCommit(false);
	st.setFetchSize(DB_FETCH_SIZE);
	return st;
    }

// * Query nom

    /**
     * The nom-query defined in the .sql-file contains multiple
     * function declarations as well as main query (for convenience
     * during development). So, extract functions, execute them,
     * then execute main query. Add any necessary extensions manually.
     *
     * @param query Function definitions, main query etc,
     * multiple lines.
     * @param st Access to nominatim db.
     * @return Result of running main query.
     */
    private static ResultSet executeNomQuery(String query, Statement st) throws SQLException {
        st.execute("CREATE EXTENSION IF NOT EXISTS postgis");
        st.execute("CREATE EXTENSION IF NOT EXISTS unaccent");
        extractAndCreateFunctions(query, st);
        return extractAndExecuteMainQuery(query, st);
    }

    /**
     * Extract functions in the multi-query and create those in the
     * nom database.
     * Format of function defs in query:
     * drop function if exists fname; create function fname(..) returns .. as $$ ... $$ language sql;
     */
    private static void extractAndCreateFunctions(String query, Statement st) throws SQLException {
        String pattern = "drop function .* language sql;";
        Matcher m = Pattern.compile(pattern, Pattern.DOTALL).matcher(query);
        while (m.find()) st.execute( m.group() );
    }

    /**
     * Extract main query in multi-query and execute.
     * Format: -- * QUERY ...end
     */
     private static ResultSet extractAndExecuteMainQuery(String query, Statement st) throws SQLException {
        query = query.replaceFirst("(?s).*-- \\* QUERY", "");
        return st.executeQuery(query);
    }

// * Key,value -> tag conversion

    /**
     * Load key-conversion-table into an ORDERED map.
     */
    private static Map<String, String[]> loadKeyConversionTable() {
        Map<String, String[]> table = new LinkedHashMap<>();
        List<String> entries = readFile("../tag-to-category-conversion/key-conversion-table");
        entries = entries.subList(1, entries.size());

        for (String entry : entries) {
            String[] blocks = entry.split(" ");
            if (blocks.length != 2 && blocks.length != 3)
                throw new RuntimeException("Bad key-conversion-table format");

            String key = blocks[0];
            String supercat = blocks[1];
            String subcat = blocks.length == 2 ? key : blocks[2];

            table.put(key, new String[]{supercat, subcat});
        }
        return table;
    }

    /**
     * Load key-value-conversion-table into a regular map.
     */
    private static Map<String, String[]> loadKeyValueConversionTable(Map<String, String[]> keyConvTable) {
        Map<String, String[]> table = new HashMap<>();
        List<String> entries = readFile("../tag-to-category-conversion/key-value-conversion-table");
        entries = entries.subList(1, entries.size());

        for (String entry :  entries) {
            String[] blocks = entry.split(" ");
            if (blocks.length != 4)
                throw new RuntimeException("Bad key-value-conversion-table format");

            String key = blocks[0];
            String value = blocks[1];
            String supercat = blocks[2];
            String subcat = blocks[3];
            if (subcat.equals("1")) subcat = key;
            else if (subcat.equals("2")) subcat = value;

            table.put(key + ":" + value, new String[]{supercat, subcat});
        }
        return table;
    }

    // * Utils

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

// * Query loca

    /**
     * @param area Return elements in this area.
     * @param popindexLimit Return elements with popindex larger than this.
     * @param count Number of elements to return.
     *
     * @return Geo-objects inside area, [lon lat]
     */
    private static List<GeoObject> queryLocaDb(Statement st, List<double[]> area, long popindexLimit) throws SQLException {
        int NO_QUIZ_ELEMS = 10;

	String sql = readLocaQueryFromFile();
        sql = String.format(sql, popindexLimit, polystr(area), NO_QUIZ_ELEMS);
	ResultSet rs = st.executeQuery(sql);

	while (rs.next()) {
	    long popindex = rs.getLong(1);
	    String name = rs.getString(2);
	    String supercat = rs.getString(3);
	    String subcat = rs.getString(4);
	    PGgeometry shape = (PGgeometry)rs.getObject(5);
	    String osmIds = rs.getString(6);

	    System.out.printf("%s. %s. %s. %s. %s\n",
	        	      popindex, name, supercat, subcat, osmIds);
	}

	return null;
    }

    /**
     * Format for postgis.
     * @param ns [lon lat]
     * @return "SRID=4326;POLYGON((lon lat,lon lat,lon lat,...))"
     */
    private static String polystr(List<double[]> ns) {
	StringBuilder sb = new StringBuilder();
	for (double[] n : ns)
	    sb.append(String.format("%s %s,", n[0], n[1]));

	String str = sb.toString();
	return "SRID=4326;POLYGON((" + str.substring(0, str.length()-1) + "))";
    }

    /**
     * @return Query for extracting data from nom-db.
     */
    private static String readLocaQueryFromFile() {
        List<String> xs = readFile("../loca_query.sql");
        StringBuilder sb = new StringBuilder();
        for (String x : xs) sb.append(x + "\n");
        return sb.toString();
    }
}
