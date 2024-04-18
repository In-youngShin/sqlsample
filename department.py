"""
File containing classes for database connection and functions to extract 
enrollment changes by year and instructor salary statistics by department. 
All results will be exported as either CSV files or PNG plots by department. 
Users can check enrollment changes for specific departments 
by specifying the department in the command line. 
Detailed instructions will be prompted while running the file.

Usage: department.py
"""
import psycopg2
import matplotlib.pyplot as plt

# GLOBAL VARIABLES FOR POSTGRE SQL DATABASE======================================================
DB_NAME  = 'university-db'    
USER     = 'inyoungshin'        
PASSWORD = ''                 
HOST     = 'localhost'       
PORT     = '5432'             
# ===============================================================================================

class DatabaseConnection:
    """
    class for a database connection. 
    with statement, It automatically establishes a connection to the database when the context is entered
    and closes the connection when the context is exited.
    """
    def __init__(self):
        """
        Initializes a new DatabaseConnection object.
        Attributes:
            connection: A psycopg2 database connection object.
            cursor: A psycopg2 cursor object for executing SQL queries.
        """
        self.connection = None
        self.cursor = None

    def __enter__(self):
        config = {
            'dbname': DB_NAME,
            'user': USER,
            'password': PASSWORD,
            'host': HOST,
            'port': PORT
        }
        self.connection = psycopg2.connect(**config)
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ Closes the database connection and commits any pending transactions.
            Args:
                exc_type: The exception type (if any) that occurred during the execution of the context block.
                exc_val: The exception value (if any) that occurred during the execution of the context block.
                exc_tb: The traceback (if any) that occurred during the execution of the context block. 
        """
        if self.connection:
            self.connection.commit()
            self.connection.close()
        if self.cursor:
            self.cursor.close()

        if exc_type or exc_val or exc_tb:
            print(f'Error: {exc_type}, {exc_val}, {exc_tb}')

# =============================================================================================== 

def write_results_to_csv(header: list, results: list, filename: str):
    """
    A helper function to write results to a csv file.
    Args:
        header (list): a list of strings representing the header of the CSV
        results (list): a list of tuples, where each tuple is a row in the csv
        filename (str): the name of the file to write to
    """
    if len(header) != len(results[0]):
        raise ValueError('The number of columns in the header must match the number of columns in each row')

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        f.write(','.join(header) + '\n')
        for row in results:
            formatted_row = [str(element) for element in row]
            f.write(','.join(formatted_row) + '\n')

def create_info_database(): 
    """
    A function to export metadata on tables in the database to a CSV file.
    """
    table_meta_query = """ SELECT s.table_name, s.column_name, s.data_type, STRING_AGG(c.constraint_name, ', ') AS constraint_names
                            FROM information_schema.columns AS s
                            LEFT JOIN information_schema.key_column_usage AS c 
                                ON s.table_name = c.table_name AND s.column_name = c.column_name
                            WHERE s.table_schema = 'public'
                            GROUP BY s.table_name, s.column_name, s.data_type Order BY s.table_name; """

    with DatabaseConnection() as cursor:
       cursor.execute(table_meta_query)
       table_meta_info = cursor.fetchall()
       list_table_meta_info = []
       for table_info in table_meta_info:
            list_table_meta_info.append(table_info)
             
       header = ["Table Name","Column Name", "Data Type", "Constraints"]
       write_results_to_csv(header, list_table_meta_info, 'table_info.csv')

def dep_enrollment(): 
    """
    A function to export department enrollment changes by year and semester to a CSV file
    """
    #SQL query statement to extract data about department enrollment changes by year and semester 
    Dep_enrollment_by_year = """ SELECT t.year, t.semester, c.dept_name, COUNT(distinct c.course_id) as total_course_num_dep, COUNT(distinct s.ID) AS total_student_enroll
                            FROM course AS c 
                            JOIN takes AS t ON t.course_id = c.course_id
                            JOIN student AS s ON s.ID = t.ID
                            GROUP BY t.year, t.semester, c.course_id, c.dept_name 
                            ORDER BY t.year, t.semester, c.dept_name; """
    list_results = []
    years = []
    semesters =[]
    dept_names =[]
    total_course_offer_num = []
    total_student_enroll = []
    
    with DatabaseConnection() as cursor:
        cursor.execute(Dep_enrollment_by_year)
        results = cursor.fetchall()
        
        for row in results: 
            list_results.append(list(row))
            years.append(row[0])
            semesters.append(row[1])
            dept_names.append(row[2])
            total_course_offer_num.append(row[3])
            total_student_enroll.append(row[4])
        
        header = ['Year', 'Semesters', 'Department', 'Num of Offered Courses', 'Num of enrolled students']
        write_results_to_csv(header, list_results, 'dep_course_stat_by_year.csv')

def spec_dep_enrollment_by_year(): 
    """
    function to creat department enrollment change by years
    Its results weill be exported to csv and png files.  
    """ 
    names =[]
    years =[]
    dept_name_query = """ SELECT dept_name FROM department; """
    years_query = """ SELECT DISTINCT year FROM teaches; """
    
    with DatabaseConnection() as cursor:
        cursor.execute(dept_name_query)
        data = cursor.fetchall()
        for row in data: 
            names.append(row[0])
        
        cursor.execute(years_query)
        year_data = cursor.fetchall()
        for y in year_data:
            years.append(y[0])
    
    # prompt to request users to specific department 
    var = input("Do you want to check department enrollment change by years? (y/n): ")
    if var.lower() in ['yes', 'y']:
        for i, name in enumerate(names): 
            num = i+1
            print(f"{num}: {name}")
        input_dept = input("Enter department number(s) to check. You can input a single number or multiple numbers (comma-separated): ")
        str_dept_inputs = input_dept.split(",")
        int_dept_inputs = [int(dep) for dep in str_dept_inputs]
        print(int_dept_inputs)
        while (max(int_dept_inputs) > len(name) and min(int_dept_inputs) < 1)or len(int_dept_inputs) == 0:
            print(max(int_dept_inputs))
            print(min(int_dept_inputs))
            print("Wrong input\n")
            print("Make sure to enter accurate department number (see below)")
            for i, name in enumerate(names): 
                print(f"{i}: {name}")
            dept = input("Enter department number(s) to check (single or multiple input, comma-separated):")
    else: 
        return # Exit function if user chooses not to see the enrollment
    input_names =[names[index] for index in int_dept_inputs] 
    
    input_dep_enrollment_by_year = """ SELECT c.dept_name, t.year, t.semester, COUNT(distinct s.ID) AS total_student_enroll
                            FROM course AS c 
                            JOIN takes AS t ON t.course_id = c.course_id
                            JOIN student AS s ON s.ID = t.ID 
                            WHERE c.dept_name IN ({})""".format(','.join(['%s'] * len(input_names)))

    input_dep_enrollment_by_year += """ GROUP BY c.dept_name, t.year, t.semester 
                ORDER BY c.dept_name, t.year, t.semester; """

    # Merge year and semester for plot labels for later use
    total_years_sems = []
    year = min(years)
    max_year = max(years)
    while year <= max_year:
        year_fall = str(year) + ' ' + 'Fall'
        total_years_sems.append(year_fall)
        year_spring = str(year) + ' ' + 'Spring'
        total_years_sems.append(year_spring)
        year += 1


    plot_data = {}
    for name in input_names: 
        plot_data[name] = {'year':[], 'enroll':[]}

    with DatabaseConnection() as cursor:
        cursor.execute(input_dep_enrollment_by_year, input_names)
        outputs = cursor.fetchall()
        print(outputs)
        for row in outputs:
            year_sem = str(row[1]) + ' ' + row[2]
            plot_data[row[0]]['year'].append(year_sem)
            plot_data[row[0]]['enroll'].append(row[3])


    for name in input_names:   
        no_miss_dep_stu_enroll=[]
        for year_sem in total_years_sems:
            if year_sem not in plot_data[name]['year']: 
                no_miss_dep_stu_enroll.append(0)
            else: 
                i = plot_data[name]['year'].index(year_sem)
                no_miss_dep_stu_enroll.append(plot_data[name]['enroll'][i])
        plot_data[name]['enroll'] = no_miss_dep_stu_enroll
        plot_data[name]['year'] = total_years_sems
    
    #create line plot for enrollment changes by department
    plt.figure(figsize=(10, 5))
    for dep_name in input_names:
        print(total_years_sems)
        print(plot_data[dep_name]['enroll'])
        plt.plot(total_years_sems, plot_data[dep_name]['enroll'], marker='o', label=dep_name)
    plt.xlabel('Year Semester')
    plt.xticks(rotation=45)
    plt.ylabel('Student Enrollment')
    plt.title('Department Student Enrollment by Year')
    plt.legend()
    plt.savefig('dept_enrollment.png')
    plt.show()
 
def dep_salary_statistics():
    """
    function to creat salary_statistics table and plots 
    containing the median, average, and standard deviation of instructor salaries by department.
    Its results weill be exported to csv and png files.  
    """

    Dep_Sal_Stat = """SELECT dept_name, COUNT(distinct ID), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary,
                avg(salary) AS average_salary, STDDEV_POP(salary) AS std_dev_salary
            FROM instructor
            GROUP BY dept_name;"""

    with DatabaseConnection() as cursor:
        cursor.execute(Dep_Sal_Stat)
        results = cursor.fetchall()
        list_results = []
        dept_names =[]
        num_instructor = []
        median_salaries =[]
        average_salaries =[]
        std_dev_salaries = []

        for row in results: 
            list_results.append(list(row))
            dept_names.append(row[0])
            num_instructor.append(row[1])
            median_salaries.append(row[2])
            average_salaries.append(row[3])
            if row[4] is not None:
                std_dev_salaries.append(row[4])
            else:
                std_dev_salaries.append(0)

        plt.figure(figsize=(10, 5))
        plt.bar(dept_names, median_salaries, color='yellow', alpha=0.7, label='Median Salary')
        plt.plot(dept_names, average_salaries, color='blue', label='Average Salary')

        for i, dept_name in enumerate(dept_names):
            plt.vlines(dept_name, average_salaries[i] - std_dev_salaries[i], average_salaries[i] + std_dev_salaries[i],
                    color='green')
        plt.plot([], [], color='green', label='Standard Deviation')

        plt.xlabel('Department')
        plt.xticks(rotation=45)
        plt.ylabel('Salary')
        plt.title('Median, Average, and Stndard Deviation of Instructor Salaries by Department')
        plt.legend()
        plt.savefig('dep_salary_stats.png')
        plt.show()

        header = ['Department', 'Numboer of Instructors', 'Median Salary',  'Average Salaries', 'Std Dev Salary']
        write_results_to_csv(header, list_results, 'dep_salary_stat.csv')
    
if __name__ == '__main__':
    create_info_database()
    dep_salary_statistics()
    dep_enrollment()
    spec_dep_enrollment_by_year()



