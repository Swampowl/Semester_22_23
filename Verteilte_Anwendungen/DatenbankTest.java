import org.mariadb.jdbc.MariaDbDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;

public class DatenbankTest {

    private static DataSource datasource = new MariaDbDataSource("jdbc:mariadb://rechentitan.dm.hs-furtwangen.de:3306/user37?user=user37&password=d83r235888a§§37");

    public static void main(String args[]) throws Exception {
        try (Connection connection =datasource.getConnection()) {
            System.out.println("\nVerbindung erfolgreich hergestellt");
            ResultSet rs = connection.createStatement().executeQuery("show databases");
            System.out.println("Verfügbare Datenbanken:");
            while (rs.next()) {
                System.out.println("- " + rs.getString("database"));
            }
        }
    }
}
