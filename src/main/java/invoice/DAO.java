package invoice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class DAO {

	private final DataSource myDataSource;

	/**
	 *
	 * @param dataSource la source de données à utiliser
	 */
	public DAO(DataSource dataSource) {
		this.myDataSource = dataSource;
	}

	/**
	 * Renvoie le chiffre d'affaire d'un client (somme du montant de ses factures)
	 *
	 * @param id la clé du client à chercher
	 * @return le chiffre d'affaire de ce client ou 0 si pas trouvé
	 * @throws SQLException
	 */
	public float totalForCustomer(int id) throws SQLException {
		String sql = "SELECT SUM(Total) AS Amount FROM Invoice WHERE CustomerID = ?";
		float result = 0;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id); // On fixe le 1° paramètre de la requête
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getFloat("Amount");
				}
			}
		}
		return result;
	}

	/**
	 * Renvoie le nom d'un client à partir de son ID
	 *
	 * @param id la clé du client à chercher
	 * @return le nom du client (LastName) ou null si pas trouvé
	 * @throws SQLException
	 */
	public String nameOfCustomer(int id) throws SQLException {
		String sql = "SELECT LastName FROM Customer WHERE ID = ?";
		String result = null;
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setInt(1, id);
			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					result = resultSet.getString("LastName");
				}
			}
		}
		return result;
	}

	/**
	 * Transaction permettant de créer une facture pour un client
	 *
	 * @param customer Le client
	 * @param productIDs tableau des numéros de produits à créer dans la facture
	 * @param quantities tableau des quantités de produits à facturer faux sinon Les deux tableaux doivent avoir la même
	 * taille
	 * @throws java.lang.Exception si la transaction a échoué
	 */
	public void createInvoice(CustomerEntity customer, int[] productIDs, int[] quantities) throws Exception {
		
                                            String sql1 = "INSERT INTO Invoice (ID ,CustomerID ,Total) VALUES(?,?,0) "; // Commande sqlpour Invoice
                                            String sql2 = "INSERT INTO Item (InvoiceID, Item, ProductID, Quantity, Cost) VALUES(?,?,?,?,?)";// Commande sql pour Item
                                            String sql3 = "SELECT Price AS PRIX FROM Product WHERE ID = ?";// Commande sql pour Product
                                            
                                            
                                            try (
                                                                  Connection connect = myDataSource.getConnection(); // Ouvrir une connexion
                                                                  
                                                                   // On précompile les requêtes SQL
			PreparedStatement stmt1 = connect.prepareStatement(sql1,Statement.RETURN_GENERATED_KEYS); 
                                                                  PreparedStatement stmt2 = connect.prepareStatement(sql2);
                                                                  PreparedStatement stmt3 = connect.prepareStatement(sql3);
                                                                 // Un ResultSet pour parcourir les enregistrements du résultat
                                                                  ResultSet resultSet = stmt3.executeQuery();
		){              
                                                                 // Début de la transaction 
                                                                 connect.setAutoCommit(false);
                                                                 try{
                                                                                       // On remplit d'abord la table Invoice : on rentre la clé auto-générer et l'id du client
                                                                                       stmt1.setInt(1, customer.getCustomerId());
                                                                                       // Execution de la réquêtre et vérification de l'auto génération de la clé :
                                                                                       int mise1 = stmt1.executeUpdate();
                                                                                       if(mise1 != 1){
                                                                                                             throw new Exception("Aucune valeur insérer.");
                                                                                       }
                                                                                       // Récupération de la clé automatiquement généré:
                                                                                       int clef = stmt1.getGeneratedKeys().getInt(1);
                                                                                       
                                                                                       // Une fois les étapes précedente effectué on remplit la table Item pour pouvoir récupérer chaque ligne de la facture
                                                                                       // Boucle for : attention cette étape nescessite que les deux tableau en paramètre soit de même longueur
                                                                                       if(productIDs.length != quantities.length){ // Vérification taille des tableaux
                                                                                                             throw new Exception("Les deux tableaux ne sont pas de la même taille.");
                                                                                       }
                                                                                       
                                                                                       for(int i = 0 ; i < quantities.length; i++){
                                                                                                             // Début de l'ajout des valeur dans Item
                                                                                                             stmt2.setInt(1, clef);
                                                                                                             stmt2.setInt(2, i);
                                                                                                             stmt2.setInt(3, productIDs[i]);
                                                                                                             stmt2.setInt(4, quantities[i]);
                                                                                                             
                                                                                                             // Récupération du prix dans la table Product
                                                                                                             stmt3.setInt(1, productIDs[i]);
                                                                                                             float prix = 0f;
                                                                                                             if(resultSet.next()){
                                                                                                                                   prix = resultSet.getFloat("PRIX");
                                                                                                             }
                                                                                                             
                                                                                                             // Fin de l'ajout des valeur dans Item
                                                                                                             stmt2.setFloat(5, prix);
                                                                                                             int mise2 = stmt2.executeUpdate();
                                                                                                              if(mise2 != 1){
                                                                                                                                    throw new Exception("Aucune valeur insérer.");
                                                                                                              }
                                                                                                             
                                                                                                             
                                                                                        }
                                                                                       
                                                                                       // Valide la transaction
                                                                                        connect.commit();
                                                                                        
                                                                  }catch (Exception ex) {
                                                                                        connect.rollback(); // On annule la transaction
                                                                                        throw ex;
                                                                 } finally { 
                                                                                        // On revient au mode de fonctionnement sans transaction
                                                                                        connect .setAutoCommit(true);
                                                                  }
                                            }
                                                                 
	}

	/**
	 *
	 * @return le nombre d'enregistrements dans la table CUSTOMER
	 * @throws SQLException
	 */
	public int numberOfCustomers() throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Customer";
		try (Connection connection = myDataSource.getConnection();
			Statement stmt = connection.createStatement()) {
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 *
	 * @param customerId la clé du client à recherche
	 * @return le nombre de bons de commande pour ce client (table PURCHASE_ORDER)
	 * @throws SQLException
	 */
	public int numberOfInvoicesForCustomer(int customerId) throws SQLException {
		int result = 0;

		String sql = "SELECT COUNT(*) AS NUMBER FROM Invoice WHERE CustomerID = ?";

		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerId);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				result = rs.getInt("NUMBER");
			}
		}
		return result;
	}

	/**
	 * Trouver un Customer à partir de sa clé
	 *
	 * @param customedID la clé du CUSTOMER à rechercher
	 * @return l'enregistrement correspondant dans la table CUSTOMER, ou null si pas trouvé
	 * @throws SQLException
	 */
	CustomerEntity findCustomer(int customerID) throws SQLException {
		CustomerEntity result = null;

		String sql = "SELECT * FROM Customer WHERE ID = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setInt(1, customerID);

			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				String name = rs.getString("FirstName");
				String address = rs.getString("Street");
				result = new CustomerEntity(customerID, name, address);
			}
		}
		return result;
	}

	/**
	 * Liste des clients localisés dans un état des USA
	 *
	 * @param state l'état à rechercher (2 caractères)
	 * @return la liste des clients habitant dans cet état
	 * @throws SQLException
	 */
	List<CustomerEntity> customersInCity(String city) throws SQLException {
		List<CustomerEntity> result = new LinkedList<>();

		String sql = "SELECT * FROM Customer WHERE City = ?";
		try (Connection connection = myDataSource.getConnection();
			PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, city);
			try (ResultSet rs = stmt.executeQuery()) {
				while (rs.next()) {
					int id = rs.getInt("ID");
					String name = rs.getString("FirstName");
					String address = rs.getString("Street");
					CustomerEntity c = new CustomerEntity(id, name, address);
					result.add(c);
				}
			}
		}

		return result;
	}
}
