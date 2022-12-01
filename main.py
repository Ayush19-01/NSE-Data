import datetime
import socket
import mysql.connector
import pandas as pd
from mysql.connector import errorcode

pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 300)
socket.setdefaulttimeout(5)

month = {"01": "JAN", "02": "FEB", "03": "MAR", "04": "APR", "05": "MAY", "06": "JUN", "07": "JUL", "08": "AUG",
         "09": "SEP", "10": "OCT", "11": "NOV", "12": "DEC"}


def create_connection():
    cnx = mysql.connector.connect(user='root', password='ayush123',
                                  database='stock')
    return cnx


def get_dates(n:int) -> list:

    base = datetime.datetime.today()
    date_list = [str(base - datetime.timedelta(days=x)) for x in range(0, n)]
    return date_list


def extract_securities_csv() -> None:
    cnx = create_connection()
    cursor = cnx.cursor()
    cursor.execute("delete from available_securities")
    df = pd.read_csv('https://archives.nseindia.com/content/equities/EQUITY_L.csv')
    dicty = df.to_dict('list')
    for i in range(len(df)):
        x, y, z = dicty["SYMBOL"][i], dicty["NAME OF COMPANY"][i], dicty[" ISIN NUMBER"][i]
        add_data = "insert into available_securities values (%s, %s, %s)"
        data = (x, y, z)
        cursor.execute(add_data,data)
    cnx.commit()
    cursor.close()
    cnx.close()

def extract_bhav_csv(dates: list) -> list:
    cnx = create_connection()
    cursor = cnx.cursor()
    cds =[]
    cursor.execute("delete from bhav")
    for i in dates:
        try:
            df = pd.read_csv(f'https://archives.nseindia.com/content/historical/EQUITIES/{i[:4]}/{month[i[5:7]]}/cm{i[8:10]}{month[i[5:7]]}{i[:4]}bhav.csv.zip')
        except TimeoutError:
            print(f"NO data found! ----> {i}")
            continue
        cds.append(i)
        dicty = df.to_dict('list')
        for i in range(len(df)):
            x, y, z, t = dicty["OPEN"][i], dicty["CLOSE"][i], dicty["ISIN"][i], dicty["TIMESTAMP"][i]
            gl = ((y-x)/x)*100
            add_data = "insert into bhav values (%s, %s, %s, %s, %s)"
            data = (x, y, t, z,gl)
            cursor.execute(add_data, data)
    cnx.commit()
    cursor.close()
    cnx.close()
    return cds


def get_top_25(dated) -> pd.DataFrame:
    cnx = create_connection()
    cursor = cnx.cursor()
    ts = dated[8:10]+"-"+month[dated[5:7]]+"-"+dated[:4]
    print(f"-----------------------------------------------------------------------------------------------------------------\n\nTop 25 gainers on {ts}\n")
    query = ("select * from available_securities as ass,bhav  where ass.isinNumber=bhav.isinNumber AND timestamp=%s order by gainlos DESC limit 25")
    cursor.execute(query, (ts,))
    data = []
    for (symbol,name,isinNumber,open,close,timestamp,x,gainlos) in cursor:
        data.append([symbol,name,isinNumber,open,close,gainlos, timestamp])
    df = pd.DataFrame(data, columns = ["SYMBOL", "NAME OF THE COMPANY", "ISINNUMBER", "OPEN", "CLOSE", "GAINS","TIMESTAMP"])
    cursor.close()
    cnx.close()
    return df

def create_tables():
    cnx = create_connection()
    cursor = cnx.cursor()
    tables = dict()
    tables['available_securities'] = (
        "CREATE TABLE `available_securities` ("
        "  `symbol` varchar(30),"
        "  `name` varchar(100),"
        "  `isinNumber` varchar(15) NOT NULL,"
        "  PRIMARY KEY (`isinNumber`)"
        ")"
    )
    tables['bhav'] = (
        "CREATE TABLE `bhav` ("
        "  `open` decimal(8,2),"
        "  `close` decimal(8,2),"
        "  `timestamp` varchar(12),"
        "  `isinNumber` varchar(15),"
        "  `gainlos` decimal(8,2)"
        ")"
    )
    for table_name in tables:
        table_description = tables[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists. Adding New data...")
                query = "delete from "+table_name
                cursor.execute(query)
            else:
                print(err.msg)
        else:
            print("OK")

    cursor.close()
    cnx.close()

def get_30_days_top_25(last: str, first: str) -> None:
    _last = last[8:10] + "-" + month[last[5:7]] + "-" + last[:4]
    _first = first[8:10]+"-"+month[first[5:7]]+"-"+first[:4]
    print(f"-----------------------------------------------------------------------------------------------------------------\n\nTop 25 gainers for Last 30 Days \n")
    cnx = create_connection()
    cursor = cnx.cursor()
    query = ("select * from available_securities AT inner join (select A.isinNumber,((B.close - A.close)*100/A.close) as Gain from (select * from bhav where timestamp=%s) A inner join (select * from bhav where timestamp= %s) B on A.isinNumber=B.isinNumber) BT on AT.isinNumber=BT.isinNumber order by BT.Gain DESC limit 25")
    cursor.execute(query, (_first,_last))
    data = []
    for (symbol, name, isinNumber, x, gain) in cursor:
        data.append([symbol, name, isinNumber, gain])
    df = pd.DataFrame(data, columns=["SYMBOL", "NAME OF THE COMPANY", "ISINNUMBER", "GAIN (%)"])
    cursor.close()
    cnx.close()
    print(df)
    return df


dates = get_dates(31)
print(dates)
create_tables()     # one time function to create tables in the database. !can be commented after one time execution
cds = extract_bhav_csv(dates) # one-time function to extract data for last 30 days !can be commented after one time execution
extract_securities_csv()    # one time function to extract the data of all securities !can be commented after one time execution
for i in dates:    # for loop to get the top 25 gainers of last 30 days
    print(get_top_25(i))


get_30_days_top_25(cds[0], cds[-1])  # to get the top gainers for last 30 days

#get_30_days_top_25("2022-12-01 18:47:32.062184","2022-11-01 18:47:32.062184")
