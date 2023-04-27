import pymysql

#Connect to the first database
db1 = pymysql.connect(
  host     = "host IP/URL",
  user     = "user_name",
  password = "user_pass",
  database = "database_name"
)
cursor1 = db1.cursor()

# Connect to the second database
db2 = pymysql.connect(
  host     = "host IP/URL",
  user     = "user_name",
  password = "user_pass",
  database = "database_name"
)

cursor2 = db2.cursor()

def getAllTables():
    # Get the list of tables in the first database
    cursor1.execute("SHOW TABLES")
    tables1 = cursor1.fetchall()

    # Get the list of tables in the second database
    cursor2.execute("SHOW TABLES")
    tables2 = cursor2.fetchall()

    table1NameList = [x[0] for x in tables1 ]
    table2NameList = [x[0] for x in tables2 ]

    print(f'Old database tables length {len(table1NameList)}')
    print(f'New database tables length {len(table2NameList)}')    
    return [table1NameList,table2NameList]

def compareTables(table1NameList,table2NameList):
    # compare which table is not present in which db
    print("\nComparing tables -------\n")
    commonTables = set(table1NameList) & set(table2NameList)
    if(len(commonTables) != len(table1NameList) or len(commonTables) != len(table2NameList) ) :
        print(f'\nTables comparison of database is "FAILED" ')
        print(f'Extra Table in DB 1 OLD {[x for x in table1NameList  if x not in commonTables]}') 
        print(f'Extra Table in DB 2 NEW {[x for x in table2NameList  if x not in commonTables]}') 
    else:
        print(f'Tables comparison of database is "PASSED" ')

def compareColumns(tableList):
    # Compare the columns in each table
    totalNoOfTables = len(tableList) # on which tables names will compare
    noOfTablesCompared = 0
    print(f'\nComparing columns started-------\n')

    for table_name in tableList:
        # Compare the columns in each table
        cursor1.execute(f"DESCRIBE {table_name}")
        columns1 = cursor1.fetchall()
        cursor2.execute(f"DESCRIBE {table_name}")
        columns2 = cursor2.fetchall()

        noOfTablesCompared += 1
        commonColumns = set(columns1) & set(columns2)
        if(len(commonColumns) != len(columns1) or len(commonColumns) != len(columns2) ) :
            print(f'\nColumns comparison of {table_name} is "FAILED" {noOfTablesCompared}/{totalNoOfTables}')
            print(f'Extra columns in OLD {table_name} {[x for x in columns1  if x not in commonColumns]}')
            print(f'Extra columns in NEW {table_name} {[x for x in columns2  if x not in commonColumns]}\n')
        else:
            print(f'Columns comparison of {table_name} is "PASSED" {noOfTablesCompared}/{totalNoOfTables}')

def compareIndexes(tableList):
    # Compare the indexes in each table
    totalNoOfTables = len(tableList) # on which tables names will compare
    noOfTablesCompared = 0
    print(f'\nComparing Indexes started-------\n')
    for table_name in tableList:
        # Compare the indexes in each table
        cursor1.execute(f"SHOW INDEXES FROM {table_name}")
        indexes1 = cursor1.fetchall()
        cursor2.execute(f"SHOW INDEXES FROM {table_name}")
        indexes2 = cursor2.fetchall()
        noOfTablesCompared += 1
        # print(indexes1)
        indexes1 = [x[:6] + x[7:] for x in indexes1]
        indexes2 = [x[:6] + x[7:] for x in indexes2]
        commonIndexes = set(indexes1) & set(indexes2)
        if(len(commonIndexes) != len(indexes1) or len(commonIndexes) != len(indexes2) ) :
            print(f'\ni\Indexes comparison of {table_name} is "FAILED" {noOfTablesCompared}/{totalNoOfTables}')
            print(f'Extra indexes in OLD {table_name} {[x for x in indexes1  if x not in commonIndexes]}')
            print(f'Extra indexes in NEW {table_name} {[x for x in indexes2  if x not in commonIndexes]}\n')
        else:
            print(f'Indexes comparison of {table_name} is "PASSED" {noOfTablesCompared}/{totalNoOfTables}')

def comparePartitions(tableList):
    # Compare the Partitions in each table
    totalNoOfTables = len(tableList) # on which tables names will compare
    noOfTablesCompared = 0
    print(f'\nComparing Partitions started-------\n')
    for table_name in tableList:
        # Compare the partitions in each table (if any)
        cursor1.execute(f"SHOW CREATE TABLE {table_name}")
        create_table1 = cursor1.fetchone()[1]
        cursor2.execute(f"SHOW CREATE TABLE {table_name}")
        create_table2 = cursor2.fetchone()[1]
        noOfTablesCompared += 1
        # Check if the table has partitioning enabled
        if "PARTITION BY" in create_table1:
            partition_info1 = create_table1.split("PARTITION BY")[1].strip()  if( "PARTITION BY" in create_table1) else "" 
            partition_info2 = create_table2.split("PARTITION BY")[1].strip()  if( "PARTITION BY" in create_table2) else "" 
            if(partition_info1 != partition_info2 ):
                print(f'\nPartitions comparison of {table_name} is "FAILED" {noOfTablesCompared}/{totalNoOfTables}')
                print(f'partition_info 1 ====> {partition_info1}')
                print(f'partition_info 2 ====> {partition_info2}')
            
        print(f'Partitions comparison of {table_name} is "PASSED" {noOfTablesCompared}/{totalNoOfTables}')


def main():
    print("Starting the script...")
    table1NameList,table2NameList = getAllTables()           # get table and format it
    commonTables = set(table1NameList) & set(table2NameList) # common tables from both table list
    compareTables(table1NameList,table2NameList)             # compare table
    compareColumns(commonTables)                             # compare columns
    compareIndexes(commonTables)                             # compare Indexes
    comparePartitions(commonTables)                          # compare Partitions
    


if __name__ == '__main__':
    main()
