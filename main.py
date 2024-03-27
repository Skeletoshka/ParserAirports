import csv
import psycopg2
from psycopg2 import Error
from os import listdir
import json
import uuid
#Путь до csv с аэропортами
csv_path = 'C:\\Users\\nbryk\\OneDrive\\Рабочий стол\\Кейс ЗИТ\\Данные'
json_path = 'C:\\Users\\nbryk\\OneDrive\\Рабочий стол\\Кейс ЗИТ\\Данные\\fact'

# Вставка данных в airport и city
try:
    connection = psycopg2.connect(user="SYSDBA",
                                  password="masterkey",
                                  host="localhost",
                                  port="5432",
                                  options="-c search_path=dbo",
                                  database="DEVELOP")
    cursor = connection.cursor()
    insert_airport_query = 'INSERT INTO airport (airport_id, airport_code, airport_name, city_id, airport_latitude, ' \
                           'airport_longitude, airport_timezone) VALUES (%s, %s, %s, %s, %s, %s, %s)'
    with open(csv_path + "\\airports.csv", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            cursor.execute("SELECT city_id FROM city WHERE city_name = %s", (row[2],))
            if len(cursor.fetchall()) == 0:
                cursor.execute("SELECT nextval('city_id_gen')")
                record_to_insert = (cursor.fetchall()[0][0], row[2])
                cursor.execute('INSERT INTO city (city_id, city_name) VALUES (%s, %s)', record_to_insert)
                connection.commit()
                print(str(row) + "\n")
    with open("airports.csv", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile, delimiter="\t")
        for row in reader:
            cursor.execute("SELECT nextval('airport_id_gen')")
            id = cursor.fetchall()[0][0]
            cursor.execute("SELECT city_id FROM city WHERE city_name = %s", (row[2],))
            city_id = cursor.fetchall()[0][0]
            record_to_insert = (id, row[0], row[1], city_id, float(row[3][1:row[3].find(',')]),
                                float(row[3][row[3].find(',') + 1:row[3].find(')')]), row[4])
            cursor.execute(insert_airport_query, record_to_insert)
            connection.commit()
            print(str(row) + "\n")
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)

files = listdir(json_path)
try:
    connection = psycopg2.connect(user="SYSDBA",
                                  password="masterkey",
                                  host="localhost",
                                  port="5432",
                                  options="-c search_path=dbo",
                                  database="DEVELOP")
    cursor = connection.cursor()
    insert_flight_query = 'INSERT INTO flight (flight_id, flight_code, company_id, airport_departure_id, airport_arrival_id, ' \
                          'flight_plandeparture, flight_planarrival, flight_factdeparture, flight_factarrival) VALUES '\
                          '(%s, %s, %s, %s, %s, %s, %s, %s, %s)'
    for f in files:
        with open(json_path + "\\" + f) as json_file:
            data = json.load(json_file)
            cursor.execute("SELECT company_id FROM company WHERE company_name = %s", (data['airline_iata_code'],))
            #Вставим перевозчика, если не было
            if len(cursor.fetchall()) == 0:
                cursor.execute("SELECT nextval('company_id_gen')")
                record_to_insert = (cursor.fetchall()[0][0], data['airline_iata_code'], str(uuid.uuid4()))
                cursor.execute('INSERT INTO company (company_id, company_name, company_token) VALUES (%s, %s, %s)',
                               record_to_insert)
                connection.commit()
            # получим ид записи
            cursor.execute("SELECT nextval('flight_id_gen')")
            id = cursor.fetchall()[0][0]
            # получим ид компании
            cursor.execute("SELECT company_id FROM company WHERE company_name = %s", (data['airline_iata_code'],))
            company_id = cursor.fetchall()[0][0]
            # получим ид аэропорта вылета
            cursor.execute("SELECT airport_id FROM airport WHERE airport_code = %s", (data['departure_airport'],))
            airport_departure_id = cursor.fetchall()[0][0]
            # получим ид аэропорта прилета
            cursor.execute("SELECT airport_id FROM airport WHERE airport_code = %s", (data['arrival_airport'],))
            airport_arrival_id = cursor.fetchall()[0][0]
            record_to_insert = (id, data['flight'], company_id, airport_departure_id, airport_arrival_id,
                                data['plan_departure'], data['plan_arrival'], data['fact_departure'], data['fact_arrival'])
            cursor.execute(insert_flight_query, record_to_insert)
            connection.commit()
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)