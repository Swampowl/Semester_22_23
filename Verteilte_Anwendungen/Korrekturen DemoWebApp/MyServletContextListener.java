package de.demo;

import java.sql.Connection;

import javax.naming.InitialContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import javax.sql.DataSource;

@WebListener
public class MyServletContextListener implements ServletContextListener {

	public void contextInitialized(ServletContextEvent sce) {
		try {
			DataSource datasource = (DataSource) (new InitialContext()).lookup("java:/comp/env/jdbc/MyDB");
			try (Connection connection = datasource.getConnection()) {
				connection.prepareStatement("select * from `products`").executeQuery();
				sce.getServletContext().setAttribute("datasource", datasource);			
				sce.getServletContext().setAttribute("shop", new Shop(datasource));			
				System.out.println("Initialisierung Demo erfolgreich");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Initialisierung Demo gescheitert");
			throw new RuntimeException("Initialisierung Demo gescheitert",e);
		}
	}

	public void contextDestroyed(ServletContextEvent sce) {
	}

}
