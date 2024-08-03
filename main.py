import pandas as pd 
import  mysql.connector as connector
import matplotlib.pyplot as plt

from config import *


def get_database_connexion():

    try:
        connexion = connector.connect(host=DB_HOST, 
                    user=DB_USER, 
                    password=DB_PASS, 
                    port=DB_PORT, 
                    database=DB_NAME)


        if connexion.is_connected(): 
            print("Yahoooooo you are connected !!!!!! ✅😁")
            return connexion

        else: 
            print("Error ❌❌❌ Database non connected!!!")
            return None

    except connector.Error as e:
        print(f"Error ❌❌❌  {e.msg}")
        return None 




        # creation du curseur 
        #cursor = connexion.cursor()

        #cursor.execute("SELECT * FROM jobs")

        #regions = cursor.fetchall()

        #df = pd.DataFrame(regions)

def get_data_from_db(cursor):
    cursor.execute("""
                   SELECT first_name, last_name, salary, hire_date, job_title, department_name
                   FROM employees 
                   INNER JOIN jobs 
                   ON employees.job_id = jobs.job_id
                   INNER JOIN departments
                   ON employees.department_id = departments.department_id
                   ORDER BY salary
                   """)
    jobs = cursor.fetchall()

    jobs_df = pd.DataFrame(jobs)
    return jobs_df
 
def clean_dataframe(df):
    df.rename(columns={
        0: "firstname", 
        1: "lastname", 
        2: "salary",
        3: "hire_date", 
        4: "job",
        5: "department"
        }, inplace=True)
    
    df["salary"] = pd.to_numeric(df['salary'])
    df["hire_date"] = pd.to_datetime(df['hire_date'])
    return df

def job_by_salary(jobs_df):

    jobs_df = jobs_df[['job', 'salary']]
    job_by_salary = jobs_df.groupby("job").sum()['salary'].sort_values()
    job_by_salary.plot(kind="barh", ylabel="emplois", xlabel="salaires")
    plt.show()


conn = get_database_connexion()

if conn is not None: 
    cursor = conn.cursor()
    df = get_data_from_db(cursor)
    df_cleaned = clean_dataframe(df)
    job_by_salary(df_cleaned)

else: 
    print("il y a eu une erreur")

# df = pd.read_csv("./datasource/customers.csv", encoding="ISO-8859-1")


