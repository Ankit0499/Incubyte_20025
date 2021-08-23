import mysql.connector
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

connection = create_db_connection("localhost", "root", "Ankit@2021", "demo")

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")



with open("information.txt") as infile:
    for line in infile:
        data = line.split("|")
        date = data[10]
        birth_year = date[4:8]
        birth_month = date[2:4]
        birth_date = date[0:2]
        d_o_b = birth_year+birth_month+birth_date
        record = (data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],0,d_o_b, data[11])
        create_customer_table = """
        CREATE TABLE if not exists """+ data[9]+""" (
          Customer_Name VARCHAR(255) PRIMARY KEY,
          Customer_Id VARCHAR(18) NOT NULL,
          Customer_Open_Date DATE,
          Last_Consulted_Date DATE,
          Vaccination_Type VARCHAR(5),
          Doctor_Consulted VARCHAR(255),
          State VARCHAR(5),
          Country VARCHAR(5),
          Post_Code INT,
          Date_of_Date DATE,
          Active_Customer VARCHAR(1)
          );
         """
        execute_query(connection, create_customer_table)
        query = ("insert into "+data[9]+" (Customer_Name, Customer_Id, Customer_Open_Date, Last_Consulted_Date, Vaccination_Type, Doctor_Consulted, State, Country, Post_Code, Date_of_Date, Active_Customer)" "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        cursor = connection.cursor()
        try:
            cursor.execute(query,record)
            connection.commit()
            print("Query successful")
        except mysql.connector.IntegrityError as err:
            print(f"Error: '{err}'")

query2 = "select * from allcustomers"
cur = connection.cursor()
try:
    cur.execute(query2)
    for row in cur:
        print(row)
    connection.commit()
    print("Query successful")
except Error as err:
    print(f"Error: '{err}'")
