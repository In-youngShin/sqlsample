# Data analysis and retrieval program for a simulated university

# Table of Contents
- [Database](#Database)
- [Programs](#Programs)
    - [The department.py program](#The-department.py-program)
    - [The course_overlap.py program](#The-course_overlap.py-program)
- [requirement](#Requriements)

## Database 
It's a a simulated university database bsaed on PostgreSQL provided by Database System Concepts, 7th Edition. 
You can download it from https://codex.cs.yale.edu/avi/db-book/university-lab-dir/sample_tables-dir/index.html.
This database includes various entities such as classrooms, departments, courses, instructors, sections, students, and more. Note that all items are randomly created for practice purposes. Each entity is represented in the form of tables with specific attributes and constraints, forming a comprehensive relational database model. For details on the database schema, refer to sql-files/DDL.sql.

To set up the database, execute the following command in your terminal or command prompt:

```
    psql university-db < DDL.sql
```
To verify the installation, execute the following commands consecutively:

``` 
    psql -d university-db
    \dt
```

## Programs 
These programs are designed for practicing data retrieval and manipulation from an SQL database using Python. They analyze and interpret complex data relationships within the database, particularly focusing on departments and courses offered over a period of 10 years within a simulated university setting.

### The-department.py-program
department.py is designed to provide analysis by departments within the university.
Users can check student enrollment changes by year across departments and instructor salary statistics by department. Additionally, department.py contains classes for database connection and functions to extract metadata about database tables and columns. Specific functions will be executed based on user input via command-line arguments.

```
Usage: python3 department.py [-h] [-metadata] [-salary] [-enrollment]

Options:
   -h, --help      Show this help message and exit
   -metadata       Export metadata about database tables and columns
   -salary         Export salary statistics by department (plot and CSV)
   -enrollment     Export enrollment changes by department (plot and CSV)
```

All results will be exported as CSV files or PNG plots by department.
 To export a plot showing enrollment changes for specific departments, 
 specify the departments as prompted in the command line. 
 Detailed instructions will be provided during execution.

### The-overlapping_course-program
course_overlap.py are desinged to find all pairs of sections that overlap
All results will be exported as CSV files.
```
Usage: python3 course_overlap.py
```

## Requriements
To run this program, Python version 3.10 or later Postgresql 16.1 or later are needed. 
Additionally, make sure you have the following Python packages installed: 
    >> argparse (version >= 1.4.0), matplotlib, and psycopg2.

All the necessary packages are listed in the requirements.txt file. 
To install them, execute the following command:
```
    pip3 install -r requirements.txt
```

