import java.sql.*;
import java.sql.Date;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;

public class Main {
    public static <MultiHashMap, Int> void main(String[] args) {
        try{
            Scanner sc= new Scanner(System.in);
            System.out.print("Enter a DataBase Name: ");
            String db= sc.nextLine();
            System.out.print("Enter Your User Name: ");
            String user_name= sc.nextLine();
            System.out.print("Enter Your Password: ");
            String password= sc.nextLine();

            Connection connect_DB = DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:3306/"+db, user_name, password);
            Statement stmt = connect_DB.createStatement();

            Connection connect_DWH = DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:3306/i191708_DWH", user_name, password);
            Statement DWH_stat = connect_DWH.createStatement();
            System.out.println("\n");

            int DB_transactions = 0;
            int DB_product = 0;
            int DB_customer = 0;
            Map <String, List<List<String>>> MultiHashMap = new HashMap<String, List<List<String>>>();
            Queue<List> Transactions_Queue = new LinkedList<>();

            int batch_no = 1;
            for(int Outer=0; Outer<206; Outer++) {
                ResultSet DB_result = stmt.executeQuery("SELECT * FROM transactions limit 50 offset "+DB_transactions);

                List<List> Queue = new LinkedList<>();
                while(DB_result.next()) {
                    int columnsNumber = DB_result.getMetaData().getColumnCount();
                    List<String> all_data = new ArrayList();
                    for (int i=1; i<=columnsNumber; i++) {
                        all_data.add(DB_result.getString(i));
                    }
                    if (!MultiHashMap.containsKey(DB_result.getString(1))){
                        List<String> Product_Data = new ArrayList();
                        List<String> Customer_Data = new ArrayList();
                        MultiHashMap.put(DB_result.getString(1), new ArrayList<List<String>>());
                        MultiHashMap.get(DB_result.getString(1)).add(all_data);
                        MultiHashMap.get(DB_result.getString(1)).add(Product_Data);
                        MultiHashMap.get(DB_result.getString(1)).add(Customer_Data);
                    }

                    List<String> list=new ArrayList<String>();
                    list.add(DB_result.getString(1));
                    list.add(DB_result.getString(2));
                    list.add(DB_result.getString(3));
                    Queue.add(list);
                }
                Transactions_Queue.add(Queue);

                ResultSet Master_data_product = stmt.executeQuery("SELECT * FROM products limit 20 offset "+DB_product);
                List<List> Master_data_product_List = new LinkedList<>(); // List to Store Product Data to Perform JOIN
                while(Master_data_product.next()) {
                    int columnsNumber = Master_data_product.getMetaData().getColumnCount();
                    List<String> all_product_data = new ArrayList<String>();
                    for (int i=1; i<=columnsNumber; i++) {
                        all_product_data.add(Master_data_product.getString(i));
                    }
                    Master_data_product_List.add(all_product_data);
                }

                ResultSet Master_data_customer = stmt.executeQuery("SELECT * FROM customers limit 10 offset "+DB_customer);
                List<List> Master_data_customer_List = new LinkedList<>(); // List to Store Customer Data to Perform JOIN
                while(Master_data_customer.next()) {
                    int columnsNumber = Master_data_customer.getMetaData().getColumnCount();
                    List<String> all_customer_data = new ArrayList<String>();
                    for (int i=1; i<=columnsNumber; i++) {
                        all_customer_data.add(Master_data_customer.getString(i));
                    }
                    Master_data_customer_List.add(all_customer_data);
                }

                // Product Join
                for (List MD: Master_data_product_List) {
                    String MD_prod = (String) MD.get(0);
                    for (List item : Transactions_Queue) {
                        for (int x = 0; x < item.size(); x++) {
                            List temp = (List<String>) item.get(x);
                            String Prod_id = (String) temp.get(1);
                            String tran_id = (String) temp.get(0);
                            if (MD_prod.equals(Prod_id)) {
                                if (MultiHashMap.get(tran_id).get(1).size() == 0) {
                                    for (int data = 1; data < (MD.size() - 1); data++) {
                                        MultiHashMap.get(tran_id).get(1).add((String) MD.get(data));
                                    }
                                    double Total_sale = Double.parseDouble(((String) MD.get(MD.size() - 1)));
                                    Total_sale *= Double.parseDouble(((String) MultiHashMap.get(tran_id).get(0).get(7)));
                                    MultiHashMap.get(tran_id).get(1).add(String.valueOf(((String) MD.get(MD.size() - 1))));
                                    MultiHashMap.get(tran_id).get(1).add(String.valueOf((Total_sale)));
                                    //System.out.println(MultiHashMap.get(tran_id));
                                }
                            }
                        }
                    }
                }

                // Customer Join
                for (List MD: Master_data_customer_List) {
                    String MD_cust = (String) MD.get(0);
                    for (List item : Transactions_Queue) {
                        for (int x = 0; x < item.size(); x++) {
                            List temp = (List<String>) item.get(x);
                            String cust_id = (String) temp.get(2);
                            String tran_id = (String) temp.get(0);
                            if (MD_cust.equals(cust_id)) {
                                if (MultiHashMap.get(tran_id).get(2).size() == 0) {
                                    MultiHashMap.get(tran_id).get(2).add((String) MD.get(MD.size()-1));
                                    //System.out.println(MultiHashMap.get(tran_id));
                                }
                            }
                        }
                    }
                }

                DB_transactions += 50;  // Total 10,000 rows
                DB_customer += 10;   // Total 50 Rows
                DB_product += 20;   // Total 100 Rows

                if (Transactions_Queue.size()==5) {
                    List Top = (List) ((LinkedList<List>) Transactions_Queue).peek();
                    //System.out.println(Top);
                    List<List> Send_List = new LinkedList<>();
                    for (int fetch = 0; fetch < Top.size(); fetch++) {
                        List temp = (List<String>) Top.get(fetch);
                        String tran_id = (String) temp.get(0);
                        Send_List.add(MultiHashMap.get(tran_id));
                        MultiHashMap.remove(tran_id);
                        //System.out.println(MultiHashMap.get(tran_id));
                    }
                    for (int f = 0; f < Send_List.size(); f++) {
                        //System.out.println(Send_List.get(f));
                        List<String> Tran = (List<String>) Send_List.get(f).get(0);
                        List<String> Product = (List<String>) Send_List.get(f).get(1);
                        List<String> Customer = (List<String>) Send_List.get(f).get(2);
                        try {
                            String Query_A = "Insert into STORE (STORE_ID, STORE_NAME) values (?, ?)";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setString(1, (String) Tran.get(3));
                            ins.setString(2, (String) Tran.get(4));
                            ins.executeUpdate();
                        } catch (Exception e) {}
                        try {
                            String Query_A = "Insert into PRODUCT (PRODUCT_ID, PRODUCT_NAME) values (?, ?)";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setString(1, (String) Tran.get(1));
                            ins.setString(2, (String) Product.get(0));
                            ins.executeUpdate();
                        } catch (Exception e) {}
                        try {
                            String Query_A = "Insert into SUPPLIER (SUPPLIER_ID, SUPPLIER_NAME) values (?, ?)";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setString(1, (String) Product.get(1));
                            ins.setString(2, (String) Product.get(2));
                            ins.executeUpdate();
                        } catch (Exception e) {}
                        try {
                            String Query_A = "Insert into CUSTOMER (CUSTOMER_ID, CUSTOMER_NAME) values (?, ?)";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setString(1, (String) Tran.get(2));
                            ins.setString(2, (String) Customer.get(0));
                            ins.executeUpdate();
                        } catch (Exception e) {}
                        try {
                            LocalDate currentDate = LocalDate.parse((String) Tran.get(6));
                            Month month = currentDate.getMonth();
                            DayOfWeek day = currentDate.getDayOfWeek();
                            String Query_A = "Insert into D_TIME (DATE_ID, DATE_I, DAY_NAME, MONTH_NAME, QUARTER_NO, YEAR) " +
                                    "values (?, ?, ?, ?, ?, ?)";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setString(1, (String) Tran.get(5));
                            ins.setDate(2, Date.valueOf((String) Tran.get(6)));
                            ins.setString(3, String.valueOf(day));
                            ins.setString(4, String.valueOf(month));
                            String month_name = String.valueOf(month);

                            if (month_name.equals("JANUARY") || month_name.equals("FEBRUARY") || month_name.equals("MARCH")) {
                                ins.setString(5, "Quarter 1");
                            } else if (month_name.equals("APRIL") || month_name.equals("MAY") || month_name.equals("JUNE")) {
                                ins.setString(5, "Quarter 2");
                            } else if (month_name.equals("JULY") || month_name.equals("AUGUST") || month_name.equals("SEPTEMBER")) {
                                ins.setString(5, "Quarter 3");
                            } else if (month_name.equals("OCTOBER") || month_name.equals("NOVEMBER") || month_name.equals("DECEMBER")) {
                                ins.setString(5, "Quarter 4");
                            }
                            ins.setInt(6, currentDate.getYear());
                            ins.executeUpdate();
                        } catch (Exception e) {}
                        // FACT Table
                        try {
                            String Query_A = "Insert into SALES (TRANSACTION_ID, STORE_ID, CUSTOMER_ID, SUPPLIER_ID, PRODUCT_ID, DATE_ID, TOTAL_QUANTITY, " +
                                    "TOTAL_SALES) values (?, ?, ?, ?, ?, ?, ?,?); ";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setInt(1, (int) Double.parseDouble(Tran.get(0)));
                            ins.setString(2, (String) Tran.get(3));
                            ins.setString(3, (String) Tran.get(2));
                            ins.setString(4, (String) Product.get(1));
                            ins.setString(5, (String) Tran.get(1));
                            ins.setString(6, (String) Tran.get(5));
                            ins.setString(7, (String) Tran.get(7));
                            ins.setString(8, (String) Product.get(4));
                            ins.executeUpdate();
                        } catch (Exception e) {
                            System.out.println(Send_List.get(f));
                        }
                    }
                    System.out.println("Batch No "+batch_no+" Send to DWH");
                    batch_no +=1;
                    ((LinkedList<List>) Transactions_Queue).pop();
                    if(DB_customer == 50){
                        DB_customer = 0;
                    }
                    if(DB_product == 100){
                        DB_product = 0;
                    }
                }
            }
            System.out.println("\nData Send To DataWareHouse Successfully !");
            connect_DWH.close();
            connect_DB.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}