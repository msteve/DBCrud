package DB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author stephenmbaalu
 */
public class DBHandler {
    

    public static final String SQL_KEYWORD="KEYWORD";
    //if interested in passing it directly here, otherwise use the second construct
    private final String URL = "jdbc:sqlserver://xxx.xx.xx.xx;databaseName=mydb;user=user1;password=xx";
    private String drivers = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private Connection conn = null;
    private Statement stmt = null;
    //  private ResultSet rs = null;
    //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    //Class.forName("com.mysql.jdbc.Driver");

    /**
     *Default Constructor
     */
    public DBHandler() {
        this.url = URL;
    }
    
    /**
     * 
     * @param url  String connection string
     */

    public DBHandler(String url) {
        this.url = url;
    }

    /**
     * Constructs the DBhandler object
     *
     * @param url String connection URL e.g
     * jdbc:sqlserver://xx.xx.xx.xx;databaseName=mydb;user=***;password=**\n
     * or jdbc:mysql://localhost/ToranUssd?user=**&password= **
     *  @param drivers String drivers e,g com.microsoft.sqlserver.jdbc.SQLServerDriver(mssql) or
     * com.mysql.jdbc.Driver (MYsql)
     *
     */
    public DBHandler(String url, String drivers) {
        this.url = url;
        this.drivers = drivers;
    }

    /**
     * select with a query as param and where Hashmap if any otherwise pass null
     *
     * @param sql String sql Query with ? for preparing statement if any
     * @param whereParams HashMap with where clause
     * @return ArrayList<> Array List of all data Items in Hash map.
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public ArrayList<HashMap<String, String>> selectQuery(String sql, HashMap<String, String> whereParams) throws SQLException, ClassNotFoundException {

        return this.finishSelect(sql, whereParams);

    }

    /**
     * Select result of the table
     *
     * @param table String table to select from
     * @return ArrayList<> ArrayList of data in hash map.
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public ArrayList<HashMap<String, String>> select(String table) throws SQLException, ClassNotFoundException {

        String sql = "SELECT * FROM " + table;

        return finishSelect(sql, null);
    }

    
    /**
     * Select result of the table with order by clause
     *
     * @param table String table to select from
     * @param orderby Order by clause e.g order by id desc
     * @return ArrayList<> ArrayList of data in hash map.
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public ArrayList<HashMap<String, String>> select(String table, String orderby) throws SQLException, ClassNotFoundException {

        String sql = "SELECT * FROM " + table + " " + orderby;

        return finishSelect(sql, null);
    }
    
     /**
     * Select result of the table with columns
     *
     * @param table String table to select from
     * @param cols  list of columns to select 
     * @return ArrayList<> ArrayList of data in hash map.
     * @throws SQLException
     * @throws ClassNotFoundException
     */

    public ArrayList<HashMap<String, String>> select(String table, ArrayList<String> cols) throws SQLException, ClassNotFoundException {

        String sql = "SELECT ";
        for (String col : cols) {
            sql += col + ",";
        }
        sql = sql.substring(0, sql.lastIndexOf(","));
        sql += " FROM " + table;
        return finishSelect(sql, null);
    }

    
     /**
     * Select result of the table with columns & order by
     *
     * @param table String table to select from
     * @param cols  list of columns to select 
     * @param orderby Order by clause e.g order by id desc
     * @return ArrayList<> ArrayList of data in hash map.
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public ArrayList<HashMap<String, String>> select(String table, ArrayList<String> cols, String orderby) throws SQLException, ClassNotFoundException {

        String sql = "SELECT ";
        for (String col : cols) {
            sql += col + ",";
        }
        sql = sql.substring(0, sql.lastIndexOf(","));
        sql += " FROM " + table + " " + orderby;
        return finishSelect(sql, null);
    }
    
     /**
     * Select result of the table with columns & where
     *
     * @param table String table to select from
     * @param cols  ArrayList of columns to select 
     * @param where  HashMap of where 
     * @return ArrayList<> ArrayList of data in hash map.
     * @throws SQLException
     * @throws ClassNotFoundException
     */

    public ArrayList<HashMap<String, String>> select(String table, ArrayList<String> cols, HashMap<String, String> where) throws SQLException, ClassNotFoundException {
        //ArrayList<HashMap<String, String>> listMap = new ArrayList<>();
        String sql = "SELECT ";
        if (cols != null) {
            for (String col : cols) {
                sql += col + ",";
            }
            sql = sql.substring(0, sql.lastIndexOf(","));
        } else {
            sql = "SELECT * ";
        }
        sql += " FROM " + table + " WHERE ";
        String[] keys = where.keySet().toArray(new String[where.size()]);

        for (String key : keys) {
            if (key.equalsIgnoreCase(SQL_KEYWORD)) {
                sql += where.get(key) + " AND ";
            } else {
                sql += key + "=? AND ";
            }
        }
        sql = sql.substring(0, sql.lastIndexOf("AND"));
        // System.out.println("New SQL:"+sql);

        return finishSelect(sql, where);
    }
    
      /**
     * Select result of the table with columns & where
     *
     * @param table String table to select from
     * @param cols  ArrayList of columns to select 
     * @param where  HashMap of where 
     * @return ArrayList<> ArrayList of data in hash map.
     * @throws SQLException
     * @throws ClassNotFoundException
     */

    /**
     * Select result of the table with columns &; where
     * @param table String table to select from
     * @param cols ArrayList of columns to select
     * @param where HashMap of where
      * @param orderby Order by clause e.g order by id desc
     * @return ArrayList<> ArrayList of data in hash map.
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public ArrayList<HashMap<String, String>> select(String table, ArrayList<String> cols, HashMap<String, String> where, String orderby) throws SQLException, ClassNotFoundException {
        //ArrayList<HashMap<String, String>> listMap = new ArrayList<>();
        String sql = "SELECT ";
        if (cols != null) {
            for (String col : cols) {
                sql += col + ",";
            }
            sql = sql.substring(0, sql.lastIndexOf(","));
        } else {
            sql = "SELECT * ";
        }
        sql += " FROM " + table + " WHERE ";
        String[] keys = where.keySet().toArray(new String[where.size()]);

        for (String key : keys) {
            if (key.equalsIgnoreCase(SQL_KEYWORD)) {
                sql += where.get(key) + " AND ";
            } else {
                sql += key + "=? AND ";
            }
        }
        sql = sql.substring(0, sql.lastIndexOf("AND"));
        // System.out.println("New SQL:"+sql);
        sql += " " + orderby;
        return finishSelect(sql, where);
    }

    private ArrayList<HashMap<String, String>> finishSelect(String sql, HashMap<String, String> where) throws SQLException, ClassNotFoundException {

        ArrayList<HashMap<String, String>> listMap = new ArrayList<>();
        if (this.establishConnection()) {

            PreparedStatement prepStatement = conn.prepareStatement(sql);
            if (where != null) {
                String[] keys = where.keySet().toArray(new String[where.size()]);
                int i = 0;
                for (String whereKey : keys) {
                    if (!whereKey.equalsIgnoreCase(SQL_KEYWORD)) {
                        prepStatement.setString((i + 1), where.get(whereKey));
                        i++;
                    }

                }
            }
            ResultSet rs = prepStatement.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            ArrayList<String> cols = new ArrayList<>();
            // The column count starts from 1
            for (int i = 1; i <= columnCount; i++) {
                String name = rsmd.getColumnName(i);
                cols.add(name);
            }

            while (rs.next()) {
                HashMap<String, String> resultMap = new HashMap<>();
                for (String cl : cols) {
                    resultMap.put(cl, rs.getString(cl));
                }
                listMap.add(resultMap);
                //  String id = rs.getString("");
            }
            rs.close();
            this.closeConnection();

        } else {
            System.out.println("\"Failed to Connect to database\"");
        }

        return listMap;
    }

    /***
     * Inserts data into a table
     * @param table String table Name
     * @param data HashMap<String, String> of data to insert
     * @return int id of the last insert item
     * @throws ClassNotFoundException
     * @throws SQLException 
     */
    public long insert(String table, HashMap<String, String> data) throws ClassNotFoundException, SQLException {

        long messageId = 0;
        if (this.establishConnection()) {
            String[] keys = data.keySet().toArray(new String[data.size()]);
            String sql = "INSERT INTO " + table + "(";// + " (session_id,session_data,phone_no) "+ " values (?,?,?)";
            String values = "(";
            for (String key : keys) {
                sql += key + ",";
                values += "?,";
            }
            sql = sql.substring(0, sql.lastIndexOf(",")) + ") VALUES ";
            values = values.substring(0, values.lastIndexOf(",")) + ")";
            // System.out.println(sql+values);
            sql = sql + values;
            System.out.println("*** Sql: ** " + sql);

            // try {
            PreparedStatement prepStatement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < keys.length; i++) {
                String key = keys[i];
                prepStatement.setString((i + 1), data.get(key));
            }
            // prepStatement.executeUpdate();  
            int affectedRows = prepStatement.executeUpdate();
            if (affectedRows != 0) {
                ResultSet generatedKeys = prepStatement.getGeneratedKeys();
                if (generatedKeys.next()) {
                    messageId = generatedKeys.getLong(1);
                }
            }
            prepStatement.close();
            this.closeConnection();
            return messageId;
            /*} catch (SQLException ex) {
             Logger.getLogger(DbHandler.class.getName()).log(Level.SEVERE, null, ex);
             new Logs().saveLog(ex.getMessage() + " " + ex.getLocalizedMessage());
             }*/
        }
        return messageId;
    }
    
    /***
     * 
     * @param table  String table name
     * @param set HashMap<String, String> set hashmap
     * @param where  HashMap<String, String> where clause
     * @return int rows affected
     * @throws ClassNotFoundException
     * @throws SQLException 
     */

    public int update(String table, HashMap<String, String> set, HashMap<String, String> where) throws ClassNotFoundException, SQLException {
        // boolean updated = false;
        int affectedRows = 0;
        String sql = "UPDATE " + table + " SET ";
        if (this.establishConnection()) {
            String[] setKeys = set.keySet().toArray(new String[set.size()]);
            String[] whereKeys = where.keySet().toArray(new String[where.size()]);

            for (String key : setKeys) {
                if (key.equalsIgnoreCase(SQL_KEYWORD)) {
                    sql += where.get(key) + ", ";
                } else {
                    sql += key + "=?,";
                }
                // sql += key + "=?,";
            }
            sql = sql.substring(0, sql.lastIndexOf(",")) + " WHERE ";

            for (String keyK : whereKeys) {
                sql += keyK + "=? AND ";
            }
            sql = sql.substring(0, sql.lastIndexOf("AND"));
            System.out.println("SQl: " + sql);
            //try {
            PreparedStatement prepStatement = conn.prepareStatement(sql);
            int i = 0;
            for (String setKey : setKeys) {
                if (!setKey.equalsIgnoreCase(SQL_KEYWORD)) {
                    prepStatement.setString((i + 1), set.get(setKey));
                    i++;
                }

            }
            for (String whereKey : whereKeys) {
                prepStatement.setString((i + 1), where.get(whereKey));
                i++;
            }
            // System.out.println(prepStatement);
            affectedRows = prepStatement.executeUpdate();
            System.out.println("ussdserver.DbHandler.updateSessionData() Rows afftected " + affectedRows);
            prepStatement.close();
            this.closeConnection();
            /* } catch (SQLException ex) {
             Logger.getLogger(DbHandler.class.getName()).log(Level.SEVERE, null, ex);
             }*/
        }
        return affectedRows;
    }

  /* 
   Some usage example
  
  public static void main(String[] args) {
        String table = "[dbo].[TestTable]";
        HashMap<String, String> data = new HashMap<>();
        data.put("key1", "SonUpdate 3");
        data.put("key2", "Son1>>45 3");
        data.put("key3", "Son2>>Updated 3");
        /* data.put("man3", "Son3");
         data.put("man4", "Son4");
         data.put("man5", "Son4");
         data.put("man6", "Son4");
         data.put("man7", "Son4");
         data.put("man8", "Son4");*/
       
       /* System.out.println("Update Running ***");
        HashMap<String, String> where = new HashMap<>();
        // where.put("id", "7");
        where.put("id", "8");
        //  where.put("key2", "Son1");
        // where.put("key4", "key1");
        // int affeacted= new DbHandler().update(table, data,where);
        // long id = new DbHandler().insert(table, data);
        // System.out.println("ID: " + id);

        System.out.println("Testing Select ");
        ArrayList<String> cols = new ArrayList<>();
        cols.add("key1");
        cols.add("key2");
        cols.add("id");

        try {
            //new DbHandler().se
            String sql = "SElect * from " + table + " where id=?";
            //new DbHandler
            ArrayList<HashMap<String, String>> result = new DbHandler().select(table);
            int i = 0;
            for (HashMap<String, String> result1 : result) {
                i++;
                System.out.println(i + " ** :" + new Gson().toJson(result1));
            }
            //See data:
        } catch (SQLException ex) {
            Logger.getLogger(DbHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DbHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }*/

    private boolean establishConnection() throws ClassNotFoundException, SQLException {
        //  try {
        //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Class.forName(this.drivers);
        //Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(url);
        
        /*conn = DriverManager
            .getConnection("jdbc:postgresql://localhost:5432/testdb",
            "postgres", "123");*/
        //conn = DriverManager.getConnection(Mysqurl);
        stmt = conn.createStatement();
        return true;
    }

    private boolean closeConnection() throws SQLException {
        // try {
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
        return true;

    }

    
}
