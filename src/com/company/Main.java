// Built on OpenJDK-15
// You might need the SQLite driver from maven
package com.company;

import java.sql.*;
import java.util.LinkedList;

abstract class Row {
    // Epoch seconds, could be used to test for cache freshness.
    private final long queried_on = System.currentTimeMillis();

    /**
     * Return the row's values as an Object array.
     */
    abstract public Object[] as_array();

    /**
     * Returns the one-column primary key of this specific row.
     */
    abstract public int get_id();
}

class TruckRow extends Row {
    private int truck_id = -1;
    private int weeks_since_last_service;
    private String engine_make;

    /**
     * If id is given, assume the row already exists
     * @param truck_id Row id of the truck in question
     * @param weeks_since_last_service Weeks since it was last serviced
     * @param engine_make Make of the truck's engine
     */
    public TruckRow(int truck_id,
                    int weeks_since_last_service,
                    String engine_make) {
        super();
        this.truck_id = truck_id;
        this.weeks_since_last_service = weeks_since_last_service;
        this.engine_make = engine_make;
    }

    /**
     * When creating a new row, we won't have an id available.
     * @param weeks_since_last_service Weeks since it was last serviced
     * @param engine_make Make of the truck's engine
     */
    public TruckRow(int weeks_since_last_service,
                    String engine_make) {
        super();
        this.weeks_since_last_service = weeks_since_last_service;
        this.engine_make = engine_make;
    }

    @Override
    public Object[] as_array() {
        Object[] arr;
        arr = new Object[]{this.truck_id,
                           this.weeks_since_last_service,
                           this.engine_make};
        return arr;
    }

    @Override
    public int get_id() {
        // No corresponding setter for this attribute; it's immutable in the data schema.
        return truck_id;
    }

    public int get_weeks_since_service() {
        return weeks_since_last_service;
    }

    public void set_weeks_since_service(int weeks) {
        // This would be a good place for a layer of input validation, mayhaps.
        // Otherwise, it's just polite encapsulation.
        this.weeks_since_last_service = weeks;
    }

    public String get_engine_make() {
        return engine_make;
    }

    public void set_engine_make(String engine_make) {
        this.engine_make = engine_make;
    }
}

abstract class Table {
    public Table(String tbl_name, String path) {
        this.tbl_name = tbl_name;
        this.db_path = path;
    }

    public final String tbl_name;
    private final String db_path;

    public abstract Row[] select_all();
    public abstract Row select_by_id(int id);

    Connection get_connection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(db_path);
        } catch (SQLException err) {
            System.out.println(err.getMessage());
        }
        return conn;
    }
}

class TruckTable extends Table {
    public TruckTable(String tbl_name, String path) {
        super(tbl_name, path);
    }

    @Override
    public Row[] select_all() {
        String sql = "SELECT * FROM " + tbl_name;
        Connection conn = super.get_connection();
        Statement statement;
        LinkedList<Row> rows = new LinkedList<>();
        ResultSet results;
        Row[] row_array = null;
        try {
            statement = conn.createStatement();
            results = statement.executeQuery(sql);

            while(results.next()) {
                // Create a new TruckRow for each item returned by the query

                int truck_id = results.getInt("truck_id");
                int weeks_since_last_service = results.getInt("weeks_since_last_service");
                String engine_make = results.getString("engine_make");

                rows.add(new TruckRow(truck_id, weeks_since_last_service, engine_make));
            }

            row_array = (TruckRow[]) rows.toArray();
            conn.close();
        } catch (SQLException err) {
            System.out.println(err.getMessage());
        }
        return row_array;
    }

    @Override
    public Row select_by_id(int id) {
        String sql = "SELECT * FROM " + tbl_name + " WHERE truck_id=" + Integer.toString(id);
        Connection conn = super.get_connection();
        Statement statement;
        TruckRow row = null;

        try {
            statement = conn.createStatement();
            ResultSet result = statement.executeQuery(sql);
            row = new TruckRow(result.getInt("truck_id"),
                               result.getInt("weeks_since_last_service"),
                               result.getString("engine_make"));
            conn.close();
        } catch (SQLException err) {
            // Or, shunt to a proper logger.
            System.out.println(err.getMessage());
        }
        return row;
    }

    public void insert_row(TruckRow new_row) {
        int new_weeks = new_row.get_weeks_since_service();
        String new_engine_make = new_row.get_engine_make();
        String sql = "INSERT INTO " + tbl_name + "(weeks_since_last_service, engine_make) VALUES (?, ?)";

        try (Connection conn = get_connection();
             PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, new_weeks);
            statement.setString(2, new_engine_make);
            statement.executeUpdate();
        } catch(SQLException err) {
            System.out.println(err.getMessage());
        }
    }
}


public class Main {
    private static final String db_url = "jdbc:sqlite:demo.db";

    /**
     * A bland little script to create the test database.
     *
     * We're not quite writing an entire database wrapper for this demo, but ideally this
     * could be incorporated into the constructor for a hypothetical DataBase object.
     * @param path The path of the target SQLite database--generally a file on disk.
     */
    private static void create_demo_table(String path) {
        // weeks_since_last_service should actually be a computed field based on a stored
        // service_date record if we're being 3rd-order normal, but this is an OOP demo
        // rather than a DB demo so we're gonna eschew date math for now.
        // For a meaningful datetime example, check my github project "esisan".
        String create_sql = "CREATE TABLE IF NOT EXISTS trucks (" +
                                "truck_id INTEGER PRIMARY KEY, " +
                                "weeks_since_last_service INTEGER, " +
                                "engine_make TEXT" +
                            ")";

        System.out.println(create_sql);
        Connection conn;
        try {
            // Create a connection and initialize the DB, if necessary.
            conn = DriverManager.getConnection(path);
            Statement create_statement = conn.createStatement();
            create_statement.executeUpdate(create_sql);
            conn.commit();
            conn.close();
        } catch (SQLException err) {
            // In the real world, shove it into an appropriate logger.
            System.out.println(err.getMessage());
        }
    }

    /**
     * Create a demo database and insert/selectt a few rows using a subclassed Table object.
     * @param args Should probably not have command line arguments, given the purpose of this project.
     */
    public static void main(String[] args) {
        create_demo_table(db_url);

        TruckTable trucks = new TruckTable("trucks", db_url);

        trucks.insert_row(new TruckRow(3, "International"));
        trucks.insert_row(new TruckRow(14, "Ford"));
        trucks.insert_row(new TruckRow(9, "Caterpillar"));

        TruckRow number_one = (TruckRow) trucks.select_by_id(1);
        System.out.println("Engine in truck number 1 is manufactured by " + number_one.get_engine_make());
    }
}
