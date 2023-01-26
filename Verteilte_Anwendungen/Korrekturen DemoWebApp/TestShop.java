package de.demo;

import java.sql.SQLException;
import java.util.Vector;

import com.fasterxml.jackson.core.JsonProcessingException;


public class TestShop {

	public static void main(String[] args) throws SQLException, JsonProcessingException {
		Shop shop = new Shop();		
		Vector<Category> categories = shop.getCategories();
		for (Category category : categories)
			System.out.println(category.getName());
	}

}
