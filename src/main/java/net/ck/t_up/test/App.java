package net.ck.t_up.test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class App {

    static String result = null;
    static Connection conn = null;
    static Statement stmt = null;
    static PreparedStatement pstmt = null;

    public static void main(String[] argv) {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/DATABASE_NAME?serverTimezone=UTC", "USERNAME", "PASSWORD");
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        try {
            stmt = conn.createStatement();
            String sql =  "DROP TABLE IF EXISTS TEMP_TAB";
            stmt.executeUpdate(sql);
            sql = "CREATE TABLE IF NOT EXISTS TEMP_TAB(";
            sql += " TCOL1 VARCHAR(128),";
            sql += " TCOL2 VARCHAR(128),";
            sql += " TCOL3 DOUBLE";
            sql += " )";
            stmt.executeUpdate(sql);

            sql = "SELECT COL1,COL2,COL3 FROM TAB";
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            sql = "INSERT INTO TEMP_TAB VALUES(?, ?, ?)";
            pstmt = conn.prepareStatement(sql);

            while (rs.next()) {
                String col1 = rs.getString("COL1");
                String col2 = rs.getString("COL2");
                Double col3 = rs.getDouble("COL3");

                pstmt.setString(1,col1);
                pstmt.setString(2,col2);
                pstmt.setDouble(3,col3);

                pstmt.addBatch();
                pstmt.clearParameters();
            }

            pstmt.executeBatch();
            pstmt.clearBatch();
            //conn.commit();

            sql = "SELECT TCOL1,TCOL2,TCOL3 FROM TEMP_TAB";

            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                String tcol1 = rs.getString("TCOL1");
                String tcol2 = rs.getString("TCOL2");
                String tcol3 = rs.getString("TCOL3");
                result += tcol1+tcol2+tcol3+"\n";
            }

            sql = "DROP TABLE IF EXISTS TEMP_TAB";
            stmt.executeUpdate(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
                stmt.close();
                conn.close();
            } catch (SQLException e){
                e.printStackTrace();
            }
        }

        FileWriter fwriter = null;

        try{
            File file = new File("output_file_name.csv");
            fwriter = new FileWriter(file,true);
            fwriter.write(result);
            fwriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fwriter.close();
            } catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
