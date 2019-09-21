import com.google.gson.*;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * Class to manage and handle all database connections
 * @author Matt Kwiatkowski
 */
public class DatabaseManager {

    /**
     * The Singleton instance of this database
     */
    private static DatabaseManager instance;

    /**
     * Incoming datatypes that have already been checked and
     * don't need to be re-checked.
     */
    private Set<String> checkedTypes;

    /**
     * Gets the singleton instance of the database manager
     * @return the only instance of {@link DatabaseManager that should exist}
     */
    public static DatabaseManager getInstance(){
        if(instance == null){
            instance = new DatabaseManager();
        }
        return instance;
    }

    /**
     * The string representing the absolute jdbc connection string for the database
     */
    private String jdbcString;

    /**
     * A queue of consumers of connections, that once a valid connection is received,
     * should execute the requred sql query.
     */
    private Queue<Consumer<Connection>> queryQueue = new ConcurrentLinkedQueue<>();

    /**
     * The one and only current connection
     */
    private static Connection currentConnection = null;


    /**
     * Private because this should be access as a singleton instance. Never directly instantiated
     */
    private DatabaseManager(){
        /* Get absolute location of a relative file on this system */
        File f = new File("data/santalucia.db");
        checkedTypes = new HashSet<>();
        jdbcString = "jdbc:sqlite:" + f.getAbsolutePath();
    }

    /**
     * Gets a usable connection to connect to the database
     * @return an {@link java.sql.Connection} that can connect you to the server.
     */
    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcString);
    }

    /**
     * Provides a connection when possible.
     * Automagically closes the connection after use.
     */
    public void getConnection(Consumer<Connection> ready){
       //System.out.println("added task.");
        queryQueue.add(ready);
        /* IMPORTANT: IMPLEMENT YOUR OWN WAY OF CALLING CHECKCONNECTION ASYNCHRONOUSLY */
        AsynchronousTaskService.process(this::checkConnection);
    }

    /**
     * Runs the next query if it should be ran.
     * Automatically iterates through queries until the end
     */
    private synchronized void checkConnection(){
       //System.out.println("checking connection!");
        if(currentConnection != null){
           //System.out.println("currentConnection isn't nul");
        }
        if(currentConnection == null && !queryQueue.isEmpty()){
           //System.out.println("running 2");
            try {
                currentConnection = getConnection();
                queryQueue.poll().accept(currentConnection);
               //System.out.println("polling.");
                currentConnection.close();
                currentConnection = null;
                checkConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * This is it. The method to end all methods.
     * This saves the given json into the database
     * Recursively looks for items that need to be given
     * a separate table
     * @param conn the given {@link Connection}
     * @param table the given datatype/name (for caching)
     * @param json the {@link com.google.gson.JsonObject} to save
     * @return the integer value of the 'rowNum' (generated or set) property of the inserted object
     */
    public int saveIntoDatabase(Connection conn, String table, JsonObject json){
       ////System.out.println("now iterating on: " + table);
        int row = -1;
        if(!json.has("rowNum")){
            Server.logger.error("COULDN't FIND THE PARAM: ROWNUM IN DATATYPE: " + table);
            return -1;
        }else{
            row = json.get("rowNum").getAsInt();
        }
        boolean ascertain =true; /*!checkedTypes.contains(datatype);*/
        /* Assert that this table exsists */
        if(ascertain){
            tableManager.assertTable(table, conn);
        }
        LinkedHashMap<String, Data> saveMap = new LinkedHashMap<>();
        /* if this object has children in the array that need a reference to the parent,
         * store it here, deal with it after the parent has been inserted and rowNum
         * has been assigned.
         */
        Set<JsonObject> children = new HashSet<JsonObject>();
        for(String s: json.keySet()){
            if(s.equals("rowNum")){
                continue;
            }
            /* Assert that this column exsists */
            JsonElement e = json.get(s);
            //System.out.println("Iteratig on: " +s);
            if(e.isJsonObject()) {
                //System.out.println("Its a json object");
                JsonObject obj = e.getAsJsonObject();
                //Just making sure its meant to be separated
                if(obj.has("rowNum")){
                   ////System.out.println("With a row num");
                    if(ascertain){
                        tableManager.assertColumn(table, s, DataType.NUMBER, conn);
                    }
                    saveObjectandReference(saveMap, obj, s, conn);
                }else{
                    //System.out.println("Without a row num");
                    //for things like 'topping' a json object consisting of ONLY primitives.
                    for(String sub: obj.keySet()){
                        JsonElement get = obj.get(sub);
                        if(get.isJsonPrimitive()){
                            JsonPrimitive r = (JsonPrimitive)get;
                            saveAndCheck(saveMap, r, table, s + "$" + sub, conn, ascertain);
                        }else{
                            System.out.println("You should add support for non row-id json objects to have other children maybe");
                        }
                    }
                }
            }else if(e.isJsonArray()){
                //TODO 2d/3d array support
               ////System.out.println("its a json array");
                /* its a list. Take care of that somehow */
                JsonArray a = (JsonArray) e;
                boolean hasChildren = false;
                //The set of types of objects in the array
                Set<String> checkTypes = new HashSet<>();
                for(JsonElement el: a){
                    if(el.isJsonObject()){
                        JsonObject subobj = (JsonObject)el;
                        //System.out.println("in the array there is a jsonObject");
                        if(!subobj.has("type")){
                            System.out.println("SUB OBJECT WITH NO TYPE DECLARATION FOUND IN ARRAY: " + s + " :" + subobj.toString());
                            continue;
                        }
                        checkTypes.add(subobj.get("type").getAsString());
                        children.add(subobj);
                        hasChildren = true;
                    }else if(el.isJsonPrimitive()){
                        JsonPrimitive r = el.getAsJsonPrimitive();
                        /* just directly save the array */
                       ////System.out.println("in the array there is a jsonPrimative. Assuming the entire thing is jsonprimatives");
                        saveAndCheck(saveMap,new Gson().toJson(a), table, s, conn, ascertain);
                        break;
                    }
                }
                if(hasChildren){
                    StringJoiner typesJoiner = new StringJoiner(",");
                    checkTypes.stream().forEach(typesJoiner::add);
                    saveMap.put(s, new Data("{check:" + checkTypes.toString() + "}", DataType.MEDIUM_STRING));
                    if(ascertain){
                        tableManager.assertColumn(table, s, DataType.SMALL_STRING, conn);
                    }
                }
            }else if(!e.isJsonNull()){
                saveAndCheck(saveMap, (JsonPrimitive)e, table, s, conn, ascertain);
                //System.out.println("its a json NULL (lame)");
            }
        }
        //System.out.println("thats all the objects in this one!");
        this.checkedTypes.add(table);
        /* Now save this object */
        if(row > 0){
            try {
                buildFromMapUpdate(saveMap, table, conn, row).execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }else{
            try {
                buildFromMapInsert(saveMap, table, conn).execute();
                /* Get the updated parents id */
                PreparedStatement ps = conn.prepareStatement("SELECT last_insert_rowid()");
                ResultSet rs = ps.executeQuery();
                row = rs.getInt(1);
                //System.out.println("got a new parent row id: " + row);
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        final int parentRow = row;
        /* Now Deal with the children (those damn children) */

        children.forEach(jsonObject -> {
            //Give children their parents id
            jsonObject.addProperty(table, parentRow);
            //Process&save the children
            saveIntoDatabase(conn, jsonObject.get("type").getAsString(), jsonObject);
        });
       ////System.out.println("finished: " + table);
        return parentRow;
    }

    /**
     * This is the other method to end all methods.
     * It will get the given data from the table,
     * given the rownum and table, and call callback
     * when the json getting is done.
     * @param conn the Connection to run this on
     * @param table the table to search
     * @param rowNum the rownNum of the item to get
     * @param callback called when the method is done getting the json
     */
    public void getFromDatabaseJson(Connection conn, String table, int rowNum, Consumer<JsonObject> callback){
            //Get the data fron the database
        try {
            //System.out.println("getting from database: " + table + " row: " + rowNum);
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + table + " WHERE rowNum=?");
            ps.setInt(1, rowNum);
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                //System.out.println("Found result.");
                callback.accept(getObjectFromResultSet(rs, conn, table, rowNum));
            }
            rs.close();
            ps.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * USed for converting sql query resultsets to json
     * Note: ResultSet#next MUST be called before this
     * @param rs the resultset used
     * @param conn the connection used
     * @param parentTable the table of which the resultset was extracted
     * @param rowNum the rowNum of the data queried
     * @return a completed JsonObject.
     */
    public JsonObject getObjectFromResultSet(ResultSet rs, Connection conn, String parentTable, int rowNum){
        try{
            ResultSetMetaData rsmd = rs.getMetaData();
            JsonObject ret = new JsonObject();
            int columnCount = rsmd.getColumnCount();
            Map<String, JsonObject> jsonObjects = new HashMap<>();
            for (int i = 1; i <= columnCount; i++ ) {
                String name = rsmd.getColumnName(i);
               ////System.out.println("found column: " + name);
                if(name.contains("$")){
                   ////System.out.println("its a primative collection object");
                    /* Its an object consisitng entirely of primatives */
                    String[] split = name.split("\\$");
                    String obj = split[0];
                    String sub = split[1];
                    if(!jsonObjects.containsKey(obj)){
                        jsonObjects.put(obj, new JsonObject());
                    }
                    Object o = rs.getObject(i);
                    if(o == null){
                        continue;
                    }
                    if(isNumber(o)){
                        jsonObjects.get(obj).addProperty(sub, (Integer)o);
                    }else{
                        jsonObjects.get(obj).addProperty(sub, o.toString());
                    }
                    continue;
                }else{
                    Object data = rs.getObject(i);
                    if(data == null){
                        ret.addProperty(name, "");
                    }else if(data.toString().startsWith("{child")){
                        //System.out.println("it has a child. Going to the child.");
                        /* its a reference. Recurse, recurse, recurse */
                        String find = data.toString().replace("{", "").replace("}", "");
                        String num = find.split(":")[1];
                        PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM " + name + " WHERE rowNum=?");
                        ps2.setInt(1, Integer.parseInt(num));
                        ResultSet rs2 = ps2.executeQuery();
                        rs2.next();
                        ret.add(name, getObjectFromResultSet(rs2, conn, name, Integer.parseInt(num)));
                        rs2.close();
                        ps2.close();
                        //getFromDatabaseJson(conn, name, Integer.parseInt(num), jsonObject -> ret.add("name", jsonObject));
                        //System.out.println("done with the child.");
                        continue;
                    }else if(data.toString().startsWith("{check")) {
                        /* Its a json array of objects */
                        //System.out.println("Issa json array!");
                        String dataString = data.toString();
                        String listString = dataString.substring(dataString.indexOf("[") + 1, dataString.indexOf("]"));
                        String[] searchTables = listString.split(", ");
                        JsonArray add = new JsonArray();
                        for (String search : searchTables) {
                            //System.out.println("searching in child table: " + search);
                            PreparedStatement ps2 = conn.prepareStatement("SELECT * FROM " + search + " WHERE " + parentTable + "=?");
                            ps2.setInt(1, rowNum);
                            ResultSet rs1 = ps2.executeQuery();
                            while (rs1.next()) {
                                //System.out.println("found a child object");
                                add.add(getObjectFromResultSet(rs1, conn, search, rs1.getInt("rowNum")));
                            }
                            ps2.close();
                            rs1.close();
                            //System.out.println("done the array.");
                        }
                        ret.add(name, add);
                        continue;
                    }else if(data.toString().startsWith("[") && data.toString().endsWith("]")){
                        /* Try parsing it as an array? */
                        try{
                            JsonArray r = new JsonParser().parse(data.toString()).getAsJsonArray();
                            ret.add(name, r);
                            continue;
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                    /* Its probably just a primative */
                    //System.out.println("its a primative.");
                    // find type
                    int type = rsmd.getColumnType(i);
                    if(type == Types.BOOLEAN){
                        ret.addProperty(name, rs.getBoolean(i));
                    }else if(type == Types.INTEGER){
                        ret.addProperty(name,rs.getInt(i));
                    }else{
                        ret.addProperty(name, rs.getString(i));
                    }
                    //System.out.println("it was added.");

                }
            }
            /* add all other jsonObject */
            jsonObjects.forEach(ret::add);
            return ret;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean isNumber(Object o){
        return o instanceof Integer ||  o instanceof Long;
    }

    /**
     * Builds a query from a given map
     * @throws SQLException
     */
    private PreparedStatement buildFromMapInsert(LinkedHashMap<String, Data> saveMap, String table,
                                                 Connection conn) throws SQLException {
        int entries = saveMap.entrySet().size();
        //Build the query
        StringBuilder questionBuild = new StringBuilder("(");
        //StringBuilder columnBuild = new StringBuilder("(");
        StringJoiner column = new StringJoiner(",", "(", ")");
        while(entries > 1){
            questionBuild.append("?,");
            entries--;
        }
        questionBuild.append("?");
        questionBuild.append(")");
        saveMap.entrySet().stream().forEach(e ->column.add(e.getKey()));
        String columnBuild = column.toString();
        String sql = "INSERT INTO " + table + " "
                + columnBuild + " VALUES " + questionBuild.toString();
        //Insert into the query
        //System.out.println("query: " + sql);
        PreparedStatement ps = conn.prepareStatement(sql);
        int param = 1;
        for (Map.Entry<String, Data> e : saveMap.entrySet()) {
            if (e.getValue().getType() == DataType.NUMBER) {
                ps.setInt(param, (int) e.getValue().getData());
            } else {
                ps.setString(param, (String) e.getValue().getData());
            }
            param++;
        }
        //System.out.println("Query: " + ps.toString());
        //It should be all built by now
        return ps;
    }

    private PreparedStatement buildFromMapUpdate(LinkedHashMap<String, Data> saveMap, String table,
                                                 Connection conn, int rowNum) throws SQLException {
        StringJoiner columnBuild = new StringJoiner(", ");

        saveMap.entrySet().stream().forEach(e ->columnBuild.add(e.getKey() + "=?"));

        //Insert into the query
        PreparedStatement ps = conn.prepareStatement("UPDATE " + table + " SET " + columnBuild.toString() + " WHERE rowNum=?");
        int param = 1;
        for (Map.Entry<String, Data> e : saveMap.entrySet()) {
            if (e.getValue().getType() == DataType.NUMBER) {
                ps.setInt(param, (int) e.getValue().getData());
            } else {
                ps.setString(param, (String) e.getValue().getData());
            }
            param++;
        }
        //Set the where clause number
        ps.setInt(param, rowNum);
        //System.out.println("Query: " + ps.toString());
        //It should be all built by now
        return ps;
    }


    /**
     * Saves the object and saves the reference to its rowid back in the parent.
     */
    private void saveObjectandReference(Map<String, Data> saveMap, JsonObject put, String name, Connection conn){
        try {
            /* it has a sub object! Recursively take care of it! */
            saveIntoDatabase(conn, name, put);
            /* Query for row number insert */
            int currentRow = put.get("rowNum").getAsInt();
            int newid;
            if(currentRow > 0){
                //System.out.println("its current row num is: " + currentRow);
                newid = currentRow;
            }else{
                PreparedStatement ps = conn.prepareStatement("SELECT last_insert_rowid()");
                ResultSet rs = ps.executeQuery();
                newid = rs.getInt(1);
                //System.out.println("its new row num is: " + newid);
                rs.close();
            }
            saveMap.put(name, new Data("{child:" + newid + "}", DataType.SMALL_STRING));
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Saves the primitive and ascertains the column if necessary
     * @param saveMap the map to save to
     * @param put the {@link JsonPrimitive} to add
     * @param table the table to insert into
     * @param column the column to put into
     * @param conn the current connection
     * @param ascertain true if the column should be checked
     */
    private void saveAndCheck(Map<String, Data> saveMap, JsonElement put, String table, String column, Connection conn, boolean ascertain){
        /* Its a regular datatype*/
        DataType d = getType(column, put);
        /* Assert the columns exsist*/
        if(ascertain){
            tableManager.assertColumn(table, column, d, conn);
        }
        if(d == DataType.NUMBER){
            saveMap.put(column, new Data(put.getAsInt(), d));
        }else{
            saveMap.put(column, new Data(put.getAsString(), d));
        }
    }

    /**
     * Saves the string and ascertains the column if necessary
     * @param saveMap the map to save to
     * @param put the string to add
     * @param table the table to insert into
     * @param column the column to put into
     * @param conn the current connection
     * @param ascertain true if the column should be checked
     */
    private void saveAndCheck(Map<String, Data> saveMap, String put, String table, String column, Connection conn, boolean ascertain){
        /* Its a regular datatype*/
        /* Assert the columns exsist*/
        saveMap.put(column, new Data(put, DataType.TEXT));
    }

    public DataType getType(String datatype, JsonElement e){
        if(e.isJsonPrimitive()){
            JsonPrimitive t = (JsonPrimitive)e;
            try{
                t.getAsInt();
                return DataType.NUMBER;
            }catch(NumberFormatException ignored){
                //ignored. Move on. Its not a number.
            }
            if(datatype.toLowerCase().contains("time") && t.getAsString().contains(":") && t.getAsString().contains("T")){
                /* Most probably a datetime*/
                return DataType.DATE;
            }
            /* then our only other option is a string */
            if(e.getAsString().length() < 128){
                return DataType.MEDIUM_STRING;
            }
            return DataType.TEXT;
        }
        return DataType.TEXT;
    }
}
