package de.demo;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import javax.inject.Singleton;
import javax.sql.DataSource;
import javax.ws.rs.BeanParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import com.mysql.cj.jdbc.MysqlDataSource;

@Singleton
@Path("/shop")
public class Shop implements ExceptionMapper<Throwable> {

	public DataSource datasource;
	
	

	public Shop(DataSource datasource) {
		this.datasource = datasource;
	}

	public Shop() {
		MysqlDataSource mdatasource = new MysqlDataSource();
        mdatasource.setURL("jdbc:mysql://localhost:3306/demo");
        mdatasource.setUser("admin");
        mdatasource.setPassword("passwort");
        this.datasource = mdatasource;
    }



	@Path("/categories")
	@GET
	@Produces({ APPLICATION_JSON })
	public Vector<Category> getCategories() throws SQLException {
		Vector<Category> categories = new Vector<Category>();
		try (Connection connection = datasource.getConnection()) {
			ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM categories");
			while (rs.next())
				categories.add(new Category(rs.getInt("id"), rs.getString("name")));
		}
		return categories;
	}

	@Path("/add-category")
	@GET
	@Produces({ TEXT_PLAIN })
	public String addCategory(@QueryParam("name") String name) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("INSERT INTO `categories` (`name`) VALUES (?); ");
			ps.setString(1, name);
			ps.execute();
		}
		return "OK";
	}

	@Path("/add-category")
	@POST
	@Produces({ TEXT_PLAIN })
	public String addCategory(@BeanParam Category category) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("INSERT INTO `categories` (`name`) VALUES (?); ");
			ps.setString(1, category.getName());
			ps.execute();
		}
		return "OK";
	}

	@Path("/delete-category")
	@GET
	@Produces({ TEXT_PLAIN })
	public String deleteCategory(@QueryParam("id") int id) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("DELETE FROM `categories` WHERE `id` = ? ");
			ps.setInt(1, id);
			ps.execute();
		}
		return "OK";
	}

	@Path("/product")
	@GET
	@Produces({ APPLICATION_JSON })
	public Product getProduct(@QueryParam("id") int id) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("SELECT * FROM products where id = ?");
			ps.setInt(1, id);
			ResultSet rs = ps.executeQuery();
			rs.next();
			int category = rs.getInt("category");
			return new Product(rs.getInt("id"), rs.getString("name"), rs.getDouble("price"), rs.getInt("stock"),
					getCategory(category));
		}
	}

	@Path("/set-product-name")
	@GET
	@Produces({ TEXT_PLAIN })
	public String setProductName(@QueryParam("id") int id, @QueryParam("name") String name) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("UPDATE products SET name = ? WHERE id = ?; ");
			ps.setString(1, name);
			ps.setInt(2, id);
			ps.execute();
		}
		return "OK";
	}

	@Path("/add-product")
	@GET
	@Produces({ TEXT_PLAIN })
	public String addProduct(@QueryParam("name") String name, @QueryParam("price") double price,
			@QueryParam("stock") int stock, @QueryParam("category") int category) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement(
					"INSERT INTO `products` (`name`, `price`, `stock`, `category`) VALUES (?, ?, ?, ?) ");
			ps.setString(1, name);
			ps.setDouble(2, price);
			ps.setInt(3, stock);
			ps.setInt(4, category);
			ps.execute();
		}
		return "OK";
	}

	@Path("/update-product")
	@GET
	@Produces({ TEXT_PLAIN })
	public String updateProduct(@QueryParam("id") int id, @QueryParam("name") String name,
			@QueryParam("price") double price, @QueryParam("stock") int stock, @QueryParam("category") int category)
			throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement(
					"UPDATE `products` SET `name` = ?, `price` = ?, `stock` = ?, `category` = ? WHERE `id` = ? ");
			ps.setString(1, name);
			ps.setDouble(2, price);
			ps.setInt(3, stock);
			ps.setInt(4, category);
			ps.setInt(5, id);
			ps.execute();
		}
		return "OK";
	}

	@Path("/add-customer")
	@GET
	@Produces({ TEXT_PLAIN })
	public String addCustomer(@QueryParam("id") int id, @QueryParam("first-name") String firstName,
			@QueryParam("second-name") String secondName, @QueryParam("street") String street,
			@QueryParam("house-number") int houseNumber, @QueryParam("zip-code") int zipCode,
			@QueryParam("city") String city, @QueryParam("phone-number") String phoneNumber,
			@QueryParam("email") String email) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement(
					"INSERT INTO `customers` (`first_name`, `second_name`, `street`, `house_number`, `zip_code`, `city`, `phone_number`, `email`) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ");
			ps.setString(1, firstName);
			ps.setString(2, secondName);
			ps.setString(3, street);
			ps.setInt(4, houseNumber);
			ps.setInt(5, zipCode);
			ps.setString(6, city);
			ps.setString(7, phoneNumber);
			ps.setString(8, email);
			ps.execute();
		}
		return "OK";
	}

	@Path("/update-customer")
	@GET
	@Produces({ TEXT_PLAIN })
	public String updateCustomer(@QueryParam("id") int id, @QueryParam("first-name") String firstName,
			@QueryParam("second-name") String secondName, @QueryParam("street") String street,
			@QueryParam("house-number") int houseNumber, @QueryParam("zip-code") int zipCode,
			@QueryParam("city") String city, @QueryParam("phone-number") String phoneNumber,
			@QueryParam("email") String email) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement(
					"UPDATE `customers` SET `first_name` = ?, `second_name` = ?, `street` = ?, `house_number` = ?, `zip_code` = ?, `city` = ?, `phone_number` = ?, `email` = ? WHERE `id` = ? ");
			ps.setString(1, firstName);
			ps.setString(2, secondName);
			ps.setString(3, street);
			ps.setInt(4, houseNumber);
			ps.setInt(5, zipCode);
			ps.setString(6, city);
			ps.setString(7, phoneNumber);
			ps.setString(8, email);
			ps.setInt(9, id);
			ps.execute();
		}
		return "OK";
	}

	@Path("/delete-product")
	@GET
	@Produces({ TEXT_PLAIN })
	public String deleteProduct(@QueryParam("id") int id) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("DELETE FROM `products` WHERE `id` = ? ");
			ps.setInt(1, id);
			ps.execute();
		}
		return "OK";
	}

	@Path("/category")
	@GET
	@Produces({ APPLICATION_JSON })
	public Category getCategory(@QueryParam("id") int id) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("SELECT * FROM categories where id = ?");
			ps.setInt(1, id);
			ResultSet rs = ps.executeQuery();
			rs.next();
			return new Category(rs.getInt("id"), rs.getString("name"));
		}
	}

	@Path("/update-category")
	@GET
	@Produces({ TEXT_PLAIN })
	public String updateCategory(@QueryParam("id") int id, @QueryParam("name") String name) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("UPDATE `categories` SET `name` = ? WHERE `id` = ? ");
			ps.setString(1, name);
			ps.setInt(2, id);
			ps.execute();
		}
		return "OK";
	}

	@Path("/products")
	@GET
	@Produces({ APPLICATION_JSON })
	public Vector<Product> getProducts() throws SQLException {
		Vector<Product> products = new Vector<Product>();
		try (Connection connection = datasource.getConnection()) {
			ResultSet rs = connection.prepareStatement("SELECT * FROM products").executeQuery();
			while (rs.next())
				products.add(new Product(rs.getInt("id"), rs.getString("name"), rs.getDouble("price"),
						rs.getInt("stock"), getCategory(rs.getInt("category"))));
		}
		return products;
	}

	@Path("/products-of-category")
	@GET
	@Produces({ APPLICATION_JSON })
	public Vector<Product> getProductsOfCategory(@QueryParam("category") int category) throws SQLException {
		Vector<Product> products = new Vector<Product>();
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("SELECT * FROM products where category = ?");
			ps.setInt(1, category);
			ResultSet rs = ps.executeQuery();
			while (rs.next())
				products.add(new Product(rs.getInt("id"), rs.getString("name"), rs.getDouble("price"),
						rs.getInt("stock"), getCategory(rs.getInt("category"))));
		}
		return products;
	}

	@Path("/customer")
	@GET
	@Produces({ APPLICATION_JSON })
	public Customer getCustomer(@QueryParam("id") int id) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("SELECT * FROM customers where id = ?");
			ps.setInt(1, id);
			ResultSet rs = ps.executeQuery();
			rs.next();
			return new Customer(rs.getInt("id"), rs.getString("first_name"), rs.getString("second_name"),
					rs.getString("street"), rs.getString("house_number"), rs.getInt("zip_code"), rs.getString("city"),
					rs.getString("phone_number"), rs.getString("email"));
		}
	}

	@Path("/customers")
	@GET
	@Produces({ APPLICATION_JSON })
	public Vector<Customer> getCustomers() throws SQLException {
		Vector<Customer> customers = new Vector<Customer>();
		try (Connection connection = datasource.getConnection()) {
			ResultSet rs = connection.prepareStatement("SELECT * FROM customers").executeQuery();
			while (rs.next())
				customers.add(new Customer(rs.getInt("id"), rs.getString("first_name"), rs.getString("second_name"),
						rs.getString("street"), rs.getString("house_number"), rs.getInt("zip_code"),
						rs.getString("city"), rs.getString("phone_number"), rs.getString("email")));
		}
		return customers;
	}

	@Path("/items")
	@GET
	@Produces({ APPLICATION_JSON })
	public Vector<Item> getItems(@QueryParam("order") int order) throws SQLException {
		Vector<Item> items = new Vector<Item>();
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("SELECT * FROM items where `order` = ?");
			ps.setInt(1, order);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				Product product = getProduct(rs.getInt("product"));
				items.add(new Item(rs.getInt("order"), product, rs.getInt("quantity")));
			}
		}
		return items;
	}

	@Path("/add-item")
	@GET
	@Produces({ TEXT_PLAIN })
	public String addItem(@QueryParam("order") int order, @QueryParam("product") int product,
			@QueryParam("quantity") int quantity) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			assertOrderNotCompleted(connection, order);
			assertProductsAvailable(connection, product, quantity);
			PreparedStatement ps = connection
					.prepareStatement("INSERT INTO `items` (`order`, `product`, `quantity`) VALUES (?,?,?); ");
			ps.setInt(1, order);
			ps.setInt(2, product);
			ps.setInt(3, quantity);
			ps.execute();
		}
		return "OK";
	}

	@Path("/delete-item")
	@GET
	@Produces({ TEXT_PLAIN })
	public String deleteItem(@QueryParam("order") int order, @QueryParam("product") int product) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			assertOrderNotCompleted(connection, order);
			PreparedStatement ps = connection
					.prepareStatement("DELETE FROM `items` WHERE `order` = ? AND `product` = ?");
			ps.setInt(1, order);
			ps.setInt(2, product);
			ps.execute();
		}
		return "OK";
	}

	@Path("/order")
	@GET
	@Produces({ APPLICATION_JSON })
	public Order getOrder(@QueryParam("id") int id) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("SELECT * FROM orders where id = ?");
			ps.setInt(1, id);
			ResultSet rs = ps.executeQuery();
			rs.next();
			Customer customer = getCustomer(rs.getInt("customer"));
			Vector<Item> items = getItems(id);
			return new Order(rs.getInt("id"), rs.getDate("time"), customer, items);
		}
	}

	@Path("/orders")
	@GET
	@Produces({ APPLICATION_JSON })
	public Vector<Order> getOrders() throws SQLException {
		Vector<Order> orders = new Vector<Order>();
		try (Connection connection = datasource.getConnection()) {
			ResultSet rs = connection.prepareStatement("SELECT id FROM orders ORDER BY id").executeQuery();
			while (rs.next())
				orders.add(getOrder(rs.getInt("id")));
		}
		return orders;
	}

	@Path("/add-order")
	@GET
	@Produces({ TEXT_PLAIN })
	public String addOrder(@QueryParam("customer") int customer) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("INSERT INTO `orders` (`customer`) VALUES (?); ");
			ps.setInt(1, customer);
			ps.execute();
		}
		return "OK";
	}

	@Path("/delete-order")
	@GET
	@Produces({ TEXT_PLAIN })
	public String deleteOrder(@QueryParam("id") int id) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("DELETE FROM `orders` WHERE `id` = ? ");
			ps.setInt(1, id);
			ps.execute();
		}
		return "OK";
	}

	@Path("/buy-order")
	@GET
	@Produces({ TEXT_PLAIN })
	public String buyOrder(@QueryParam("id") int id) throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			PreparedStatement ps = connection.prepareStatement("UPDATE `orders` SET `time` = ? WHERE `id` = ? ");
			Date now = new Date();
			ps.setTimestamp(1, new java.sql.Timestamp(now.getTime()));
			ps.setInt(2, id);
			int n = ps.executeUpdate();
		}
		return "OK";
	}

	@Override
	public Response toResponse(Throwable exception) {
		exception.printStackTrace();
		System.out.println("REST-Exception: " + exception.getMessage());
		return Response.status(500).entity(exception.getMessage()).type("text/plain").build();
	}

	public void assertOrderNotCompleted(Connection connection, int order) throws SQLException {
		PreparedStatement ps = connection.prepareStatement("SELECT `time` FROM `orders` WHERE `id` = ?");
		ps.setInt(1, order);
		ResultSet rs = ps.executeQuery();
		boolean b = rs.next();
		if (!b)
			throw new ShopException("Diese Operation kann nicht ausgeführt werden, da die Bestellung #" + order
					+ " nicht in der Datenbank ist.");
		String t = rs.getString("time");
		if (t != null)
			throw new ShopException("Diese Operation kann nicht ausgeführt werden, da die Bestellung #" + order
					+ " nicht mehr offen ist.");
	}

	public void assertProductsAvailable(Connection connection, int product, int quantity) throws SQLException {
		String sql = "select `products`.`name` as `product`, SUM(`items`.`quantity`) as `inorders`,  `products`.`stock` as `instock` from `products`, `orders`, `items` WHERE `items`.`product` = `products`.`id` AND `items`.`order` = `orders`.`id` AND `products`.`id` = ? GROUP BY `products`.`id` ORDER BY `products`.`name`";
		PreparedStatement ps = connection.prepareStatement(sql);
		ps.setInt(1, product);
		ResultSet rs = ps.executeQuery();
		rs.next();
		String productName = rs.getString("product");
		int inorders = rs.getInt("inorders");
		int instocks = rs.getInt("instock");
		if (inorders + quantity > instocks)
			throw new ShopException("Diese Operation kann nicht ausgeführt werden, da nicht genügend Produkte '"
					+ productName + "' auf Lager sind. Verfügbar sind aktuell nur " + (instocks - inorders) + " Stück.");
	}

	public Vector<Integer> getOrderIds(Connection connection) throws SQLException {
		Vector<Integer> orders = new Vector<Integer>();
		ResultSet rs = connection.prepareStatement("SELECT id FROM orders ORDER BY id").executeQuery();
		while (rs.next())
			orders.add(rs.getInt("id"));
		return orders;
	}

	public Vector<Category> getCategoriesWithItems(Connection connection) throws SQLException {
		Vector<Category> categories = new Vector<Category>();
		ResultSet rs = connection.createStatement().executeQuery(
				"select distinct `categories`.`id` as `id`, `categories`.`name` as `name` from `orders`, `items`, `categories`, `products` where `orders`.`id` = `items`.`order` and `items`.`product` = `products`.`id` and `products`.`category` = `categories`.`id` ORDER BY `categories`.`name`");
		while (rs.next()) {
			categories.add(new Category(rs.getInt("id"), rs.getString("name")));
		}
		return categories;
	}

	public ChartTable1D getStocksByCategories() throws SQLException {
		try (Connection connection = datasource.getConnection()) {
			Vector<String> names = new Vector<String>();
			Vector<Double> amounts = new Vector<Double>();
			ResultSet rs = connection.createStatement().executeQuery(
					"SELECT categories.name as category, SUM(products.stock*products.price) as amount FROM products, categories WHERE products.category = categories.id GROUP BY products.category ORDER BY products.category");
			while (rs.next()) {
				names.add(rs.getString("category"));
				amounts.add(rs.getDouble("amount"));
			}
			double[] values = new double[amounts.size()];
			for (int i = 0; i < amounts.size(); i++) {
				values[i] = amounts.elementAt(i);
			}
			return new ChartTable1D(names.stream().toArray(String[]::new), values);
		}
	}

	public ChartTable2D getSumOrderItems() throws SQLException {

		try (Connection connection = datasource.getConnection()) {
			connection.setAutoCommit(false);
			connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			String sql = "select `products`.`name` as `product`, SUM(`items`.`quantity`) as `inorders`,  `products`.`stock` as `instock` from `products`, `orders`, `items` WHERE `items`.`product` = `products`.`id` AND `items`.`order` = `orders`.`id` GROUP BY `products`.`id` ORDER BY `products`.`name`";
			ResultSet rs = connection.prepareStatement(sql).executeQuery();
			Vector<Double> inorders = new Vector<Double>();
			Vector<Double> instock = new Vector<Double>();
			Vector<String> productNames = new Vector<String>();
			int counter = 0;
			while (rs.next()) {
				inorders.add(rs.getDouble("inorders"));
				instock.add(rs.getDouble("instock"));
				productNames.add(rs.getString("product"));
				counter++;
			}
			double[][] values = new double[2][counter];
			for (int j=0; j<counter; j++) {
				values[0][j] = inorders.elementAt(j);
				values[1][j] = instock.elementAt(j);
			}
			return new ChartTable2D(new String[] { "in Bestellungen", "auf Lager" }, productNames.stream().toArray(String[]::new), values);
		}
	}

	public ChartTable2D getSumOrderCategories() throws SQLException {
		double[][] values = new double[0][0];

		try (Connection connection = datasource.getConnection()) {
			connection.setAutoCommit(false);
			connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			Vector<Category> categories = getCategoriesWithItems(connection);
			Hashtable<Integer, Integer> categoriesHash = new Hashtable<Integer, Integer>();
			for (int i = 0; i < categories.size(); i++)
				categoriesHash.put(categories.elementAt(i).getId(), i);
			Vector<Integer> orders = getOrderIds(connection);
			Hashtable<Integer, Integer> ordersHash = new Hashtable<Integer, Integer>();
			for (int i = 0; i < orders.size(); i++)
				ordersHash.put(orders.elementAt(i), i);
			values = new double[orders.size()][categories.size()];
			String sql = "select  `orders`.`id` as `order`, `categories`.`id` as `category`, sum(`products`.`price` * `items`.`quantity`) as `value` from `orders`, `items`, `products`, `categories` where `items`.`order` = `orders`.`id` and `products`.`category` = `categories`.`id` and `items`.`product` = `products`.`id` group by `orders`.`id`, `categories`.`id`";
			ResultSet rs = connection.createStatement().executeQuery(sql);
			while (rs.next()) {
				int ord = ordersHash.get(rs.getInt("order"));
				int cat = categoriesHash.get(rs.getInt("category"));
				double v = rs.getDouble("value");
				values[ord][cat] = v;
			}
			String[] labelsA = orders.stream().map(x -> "Bestellung #" + x).toArray(String[]::new);
			String[] labelsB = categories.stream().map(x -> x.getName()).toArray(String[]::new);
			return new ChartTable2D(labelsA, labelsB, values);
		}
	}

}
