/**
 * This file is part of mongodb-snippets.
 *
 * mongodb-snippets is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2, or (at your option) any later version.
 *
 * mongodb-snippets is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; see the file COPYING. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package es.devcircus.mongodb_snippets.hello_world;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Adrian Novegil Toledo
 * @mail adrian.novegil@gmail.com
 * @description
 */
public class Main {

    private static DB db;
    private final static String DB_NAME = "mydb";
    private final static String TEST_COLLECTION = "testCollection";

    /**
     * Método main.
     *
     * @param args Array de argumentos del programa.
     */
    public static void main(String[] args) {

        //Making A Connection
        makingAConnection();
        
        //Inserting a Document
        insertIngADocument();

        //Getting A List Of Collections
        gettingAListOfCollections();
        
        //Getting A Collection
        gettingACollection();

        //Finding the First Document In A Collection using findOne()
        findingTheFirstDocumentInACollectionUsingFindOne();

        //Adding Multiple Documents
        addingMultipleDocuments();

        //Counting Documents in A Collection
        countingDocumentsInACollection();

        //Using a Cursor to Get All the Documents
        usingACursorToGetAllTheDocuments();

        //Getting A Single Document with A Query
        gettingASingleDocumentWithAQuery();

        //Getting A Set of Documents With a Query
        gettingASetOfDocumentsWithAQuery();

        //Creating An Index
        creatingAnIndex();

        //Getting a List of Indexes on a Collection
        gettingAListOfIndexesOnACollection();

        //Quick Tour of the Administrative Functions
        quickTourOfTheAdministrativeFunctions();

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Fin                                                           ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();


    }

    /**
     * Método que nos permite crear una nueva conexión con nuestra base de 
     * datos.
     */
    public static void makingAConnection() {
        try {
            System.out.println("---------------------------------------------------------------");
            System.out.println(" Making A Connection                                           ");
            System.out.println("---------------------------------------------------------------");
            System.out.println();

            /*Making A Connection
             To make a connection to a MongoDB, you need to have at the minimum, 
             * the name of a database to connect to. The database doesn't have 
             * to exist - if it doesn't, MongoDB will create it for you.
             Additionally, you can specify the server address and port when connecting. 
             * The following example shows three ways to connect to the database 
             * mydb on the local machine :*/

            Mongo m = new Mongo();
            // or
            //Mongo m = new Mongo( "localhost" );
            // or
            //Mongo m = new Mongo( "localhost" , 27017 );

            db = m.getDB(DB_NAME);

            System.out.println(" Conexión establecida..: " + db.getName());

            /*At this point, the db object will be a connection to a MongoDB 
             * server for the specified database. With it, you can do further 
             * operations. 
            
             Note: The Mongo object instance actually represents a pool of connections 
             * to the database; you will only need one object of class Mongo 
             * even with multiple threads.  See the concurrency doc page for more 
             * information.
            
             The Mongo class is designed to be thread safe and shared among threads. 
             * Typically you create only 1 instance for a given DB cluster and 
             * use it across your app. If for some reason you decide to create 
             * many mongo intances, note that:
            
             all resource usage limits (max connections, etc) apply per mongo instance
             to dispose of an instance, make sure you call mongo.close() to clean 
             * up resources*/
        } catch (UnknownHostException | MongoException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Método que recupear la lista de colecciones que hay en nuestra base de 
     * datos.
     */
    public static void gettingAListOfCollections() {

        System.out.println("---------------------------------------------------------------");
        System.out.println(" Getting A List Of Collections                                 ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Getting A List Of Collections
         Each database has zero or more collections. You can retrieve a list 
         * of them from the db (and print out any that are there) :*/

        Set<String> colls = db.getCollectionNames();

        System.out.println(" Listado de colecciones de la base de datos:\n");
        for (String s : colls) {
            System.out.println("  - " + s);
        }
        System.out.println();
    }

    /**
     * Método que nos permite recuperar una instancia de colección.
     */
    public static void gettingACollection() {

        System.out.println("---------------------------------------------------------------");
        System.out.println(" Getting A Collection                                          ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Getting A Collection
         To get a collection to use, just specify the name of the collection 
         * to the getCollection(String collectionName) method:*/

        System.out.print(" Recuperando colección ... ");
        DBCollection coll = db.getCollection(TEST_COLLECTION);
        System.out.println("[OK]");

        /*Once you have this collection object, you can now do things like 
         * insert data, query for data, etc*/

    }

    /**
     * Método que inserta un nuevo documento en la base de datos.
     */
    public static void insertIngADocument() {

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Inserting a Document                                          ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /* Inserting a Document
         *             
         * Once you have the collection object, you can insert documents into 
         * the collection. For example, lets make a little document that in 
         * JSON would be represented as*/

        /*{
         "name" : "MongoDB",
         "type" : "database",
         "count" : 1,
         "info" : {
         x : 203,
         y : 102
         }
         }*/

        /*Notice that the above has an "inner" document embedded within it. 
         * To do this, we can use the BasicDBObject class to create the 
         * document (including the inner document), and then just simply 
         * insert it into the collection using the insert() method.*/
        
        System.out.print(" Insertando elemento ...");
        
        DBCollection coll = db.getCollection(TEST_COLLECTION);

        BasicDBObject doc = new BasicDBObject();

        doc.put("name", "MongoDB");
        doc.put("type", "database");
        doc.put("count", 1);

        BasicDBObject info = new BasicDBObject();

        info.put("x", 203);
        info.put("y", 102);

        doc.put("info", info);

        coll.insert(doc);

        System.out.println("[OK]\n");

    }

    /**
     * Método que nos permite recupear el primer documento de una colección 
     * determinada, empleando para ello el método finOne.
     */
    public static void findingTheFirstDocumentInACollectionUsingFindOne() {

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Finding the First Document In A Collection using findOne()    ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Finding the First Document In A Collection using findOne()
         To show that the document we inserted in the previous step is there, 
         * we can do a simple findOne() operation to get the first document 
         * in the collection. This method returns a single document (rather 
         * than the DBCursor that the find() operation returns), and it's 
         * useful for things where there only is one document, or you are 
         * only interested in the first. You don't have to deal with the 
         * cursor.*/

        DBCollection coll = db.getCollection(TEST_COLLECTION);

        DBObject myDoc = coll.findOne();
        System.out.print(" Información del elemento..: " + myDoc + "\n");

        /*and you should see*/

        /*{ "_id" : "49902cde5162504500b45c2c" , "name" : "MongoDB" , 
         * "type" : "database" , "count" : 1 , "info" : { "x" : 203 , "y" : 102}}
         */

        /*Note the _id element has been added automatically by MongoDB to your 
         * document. Remember, MongoDB reserves element names that start 
         * with "_"/"$" for internal use.*/

    }

    /**
     * Método que añade múltiples documentos a nuestra colección dentro de la 
     * base de datos.
     */
    public static void addingMultipleDocuments() {

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Adding Multiple Documents                                     ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Adding Multiple Documents
         In order to do more interesting things with queries, let's add multiple 
         * simple documents to the collection. These documents will just be*/

        /*{
         "i" : value
         }*/

        /*and we can do this fairly efficiently in a loop*/

        System.out.print(" Insertando elementos ...");

        DBCollection coll = db.getCollection(TEST_COLLECTION);

        for (int i = 0; i < 10; i++) {
            coll.insert(new BasicDBObject().append("i", i));
        }
        
        System.out.println("[OK]");

        /*Notice that we can insert documents of different "shapes" into the 
         * same collection. This aspect is what we mean when we say that 
         * MongoDB is "schema-free"*/

    }

    /**
     * Método que cuenta el número de documentos que hay en una colección 
     * determinada de nuestra base de datos.
     */
    public static void countingDocumentsInACollection() {

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Counting Documents in A Collection                            ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Counting Documents in A Collection
         Now that we've inserted 101 documents (the 100 we did in the loop, 
         * plus the first one), we can check to see if we have them all using 
         * the getCount() method.*/

        DBCollection coll = db.getCollection(TEST_COLLECTION);

        System.out.println(" Número de documentos..: " + coll.getCount());

        /*and it should print 101.*/

    }

    /**
     * Método que cuenta todos los documentos de una colección empleando para 
     * ello un cursor.
     */
    public static void usingACursorToGetAllTheDocuments() {

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Using a Cursor to Get All the Documents                       ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Using a Cursor to Get All the Documents
         * In order to get all the documents in the collection, we will use the 
         * find() method. The find() method returns a DBCursor object which 
         * allows us to iterate over the set of documents that matched our 
         * query. So to query all of the documents and print them out :*/

        DBCollection coll = db.getCollection(TEST_COLLECTION);

        DBCursor cur = coll.find();

        while (cur.hasNext()) {
            System.out.println(" - " + cur.next());
        }

        /*and that should print all 101 documents in the collection.*/

    }

    /**
     * Método que recupera un documento determinado empleando para ellos una 
     * consulta elaborada por nosotros.
     */
    public static void gettingASingleDocumentWithAQuery() {

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Getting a Single Document with A Query                        ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Getting A Single Document with A Query
         * We can create a query to pass to the find() method to get a subset 
         * of the documents in our collection. For example, if we wanted to 
         * find the document for which the value of the "i" field is 71, we 
         * would do the following ;*/

        DBCollection coll = db.getCollection(TEST_COLLECTION);

        DBCursor cur = coll.find();

        BasicDBObject query = new BasicDBObject();

        query.put("i", 7);

        cur = coll.find(query);

        while (cur.hasNext()) {
            System.out.println(" - " + cur.next());
        }

        /*and it should just print just one document
        
         { "_id" : "49903677516250c1008d624e" , "i" : 71 }
         You may commonly see examples and documentation in MongoDB which use $ Operators, such as this:
        
         db.things.find({j: {$ne: 3}, k: {$gt: 10} });
        
         These are represented as regular String keys in the Java driver, using embedded DBObjects:*/

        query = new BasicDBObject();

        query.put("j", new BasicDBObject("$ne", 3));
        query.put("k", new BasicDBObject("$gt", 10));

        cur = coll.find(query);

        while (cur.hasNext()) {
            System.out.println(" Información del documento..: " + cur.next());
        }
    }

    /**
     * Recuperamos una colección de objetos documento empleando para ello una 
     * consulta elaborad por nosotros.
     */
    public static void gettingASetOfDocumentsWithAQuery() {

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Getting a Set of Documents With a Query                       ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Getting A Set of Documents With a Query
         We can use the query to get a set of documents from our collection. 
         * For example, if we wanted to get all documents where "i" > 5, we 
         * could write :*/

        DBCollection coll = db.getCollection(TEST_COLLECTION);

        DBCursor cur = coll.find();

        BasicDBObject query = new BasicDBObject();

        query.put("i", new BasicDBObject("$gt", 5));  // e.g. find all where i > 5

        cur = coll.find(query);

        System.out.println(" Elementos que cumplen, i > 5\n");
        while (cur.hasNext()) {
            System.out.println("  - " + cur.next());
        }

        /*which should print the documents where i > 50. We could also get a 
         * range, say 2 < i <= 7 :*/

        System.out.println();

        query = new BasicDBObject();

        query.put("i", new BasicDBObject("$gt", 2).append("$lte", 7));  // i.e.   2 < i <= 7

        cur = coll.find(query);

        System.out.println(" Elementos que cumplen, 2 < i <= 7\n");
        while (cur.hasNext()) {
            System.out.println("  - " + cur.next());
        }
    }

    /**
     * Método que crea un índice.
     */
    public static void creatingAnIndex() {

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Creating An Index                                             ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Creating An Index
         MongoDB supports indexes, and they are very easy to add on a collection. 
         * To create an index, you just specify the field that should be indexed, 
         * and specify if you want the index to be ascending (1) or descending 
         * (-1). The following creates an ascending index on the "i" field :*/

        DBCollection coll = db.getCollection(TEST_COLLECTION);
        
        System.out.print(" Creando índice ...");
        // create index on "i", ascending
        coll.createIndex(new BasicDBObject("i", 1));  
        System.out.println("[OK]");

    }

    /**
     * Recuperamos la lista de índices de una colección concreta.
     */
    public static void gettingAListOfIndexesOnACollection() {

        System.out.println();
        System.out.println("---------------------------------------------------------------");
        System.out.println(" Getting a List of Indexes on a Collection                     ");
        System.out.println("---------------------------------------------------------------");
        System.out.println();

        /*Getting a List of Indexes on a Collection
         You can get a list of the indexes on a collection :*/

        DBCollection coll = db.getCollection(TEST_COLLECTION);

        List<DBObject> list = coll.getIndexInfo();

        for (DBObject o : list) {
            System.out.println(" - " + o);
        }

        /*and you should see something like
         { "name" : "i_1" , "ns" : "mydb.testCollection" , "key" : { "i" : 1} }*/
    }

    /**
     * Método en el que se muestran algunas funciones de administración.
     */
    public static void quickTourOfTheAdministrativeFunctions() {
        try {
            System.out.println();
            System.out.println("---------------------------------------------------------------");
            System.out.println(" Quick Tour of the Administrative Functions                    ");
            System.out.println("---------------------------------------------------------------");
            System.out.println();

            /*Quick Tour of the Administrative Functions
             Getting A List of Databases
             You can get a list of the available databases:*/

            Mongo m = new Mongo();

            for (String s : m.getDatabaseNames()) {
                System.out.println(" - " + s);
            }

            /*Dropping A Database
             You can drop a database by name using the Mongo object:*/

            m.dropDatabase(DB_NAME);
        } catch (UnknownHostException | MongoException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
