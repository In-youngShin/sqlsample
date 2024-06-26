"""
Program for finding overlapping sections

This program creates a new table called overlapping_sections that 
contains all pairs of sections that overlap and exports their data to a CSV file.

For each pair of overlapping sections, 
their course ID, section ID, and the time range during which the sections overlap will be exported to the CSV file.

For example, consider the following two sections that overlap on Monday from 10:00 to 10:15:
    Course: CPSC-437-001, Day: Monday, Year: 2017, Semester: Fall, Time: 09:00-10:15
    Course: CPSC-237-002, Day: Monday, Year: 2017, Semester: Fall, Time: 10:00-10:45
"""
from typing import TypedDict

from department import DatabaseConnection
from department import write_results_to_csv

def create_overlapping_sections_table_if_not_exists():
    """
    Create the overlapping_sections table if it does not exist.
    """
    check_exist_query = """SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'overlapping_sections'
    );"""

    create_table_query = """ CREATE TABLE overlapping_sections (
        day VARCHAR(8),
        course_id_1 varchar(8),
        sec_id_1 varchar(8),
        year_1 numeric(4,0),
        semester_1 varchar(6),
        course_id_2 varchar(8),
        sec_id_2 varchar(8),
        year_2 numeric(4,0),
        semester_2 varchar(6),
        overlap_time_start varchar(5),
        overlap_time_end varchar(5), 
        primary key (day, course_id_1, sec_id_1, semester_1, year_1, course_id_2, sec_id_2, semester_2, year_2) );"""

    join_section_time1 = """ FROM (SELECT S.course_id, S.sec_id, S.year, S.semester, S.time_slot_id, T.day, T.start_hr, T.start_min, T.end_hr, T.end_min FROM section AS S JOIN time_slot AS T ON S.time_slot_id = T.time_slot_id) AS s1,  """
    join_section_time2 = """ (SELECT S.course_id, S.sec_id, S.year, S.semester, S.time_slot_id, T.day, T.start_hr, T.start_min, T.end_hr, T.end_min FROM section AS S JOIN time_slot AS T ON S.time_slot_id = T.time_slot_id) AS s2 """

    two_section_join = """SELECT s1.day, s1.course_id, s1.sec_id, s1.year, s1.semester,
                                s1.start_hr, s1.start_min, s1.end_hr, s1.end_min,
                                s2.course_id, s2.sec_id, s2.year, s2.semester,
                                s2.start_hr, s2.start_min, s2.end_hr, s2.end_min """
    two_section_join += join_section_time1 
    two_section_join += join_section_time2          
    two_section_join += """ WHERE s1.day = s2.day AND s1.semester = s2.semester AND s1.year=s2.year  AND ((s1.course_id < s2.course_id) OR (s1.course_id = s2.course_id AND s1.sec_id < s2.sec_id)); """
    
    with DatabaseConnection() as cursor:
        cursor.execute(check_exist_query)
        if not cursor.fetchone()[0]:

            cursor.execute(create_table_query)
            cursor.execute(two_section_join)
            results = cursor.fetchall()
            for elm in results: 
                elm = list(elm)
            header=["day", "course_id_1", "sec_id_1", "year_1", "semester_1", "s1.start_hr", "s1.start_min", "s1.end_hr", "s1.end_min","course_id_2", "sec_id_2", "year_2", "sem_2", "s2.start_hr", "s2.start_min", "s2.end_hr", "s2.end_min"]
            write_results_to_csv(header, results, 'task2_sub.csv')


            for row in results:
                slot1 = {'day': row[0], 'start_hr': row[5], 'start_min': row[6], 'end_hr': row[7], 'end_min': row[8]}
                slot2 = {'day': row[0], 'start_hr': row[13], 'start_min': row[14], 'end_hr': row[15], 'end_min': row[16]}

                overlap_result = is_overlap(slot1, slot2)

                if overlap_result:
                    insert_overlap_query = f"""INSERT INTO overlapping_sections ( day, course_id_1,sec_id_1 ,year_1, semester_1, course_id_2 ,sec_id_2 , year_2 ,semester_2 ,overlap_time_start,overlap_time_end)
                                               VALUES (
                                                   '{row[0]}', '{row[1]}', '{row[2]}', {row[3]}, '{row[4]}',
                                                   '{row[9]}', '{row[10]}', {row[11]}, '{row[12]}',
                                                   '{overlap_result[0]}', '{overlap_result[1]}');"""
                                               
                    cursor.execute(insert_overlap_query)

# utility type for time slots
TimeSlotInfo = TypedDict('TimeSlotInfo',
                         {'day': str,
                          'start_hr': int,
                          'start_min': int,
                          'end_hr': int,
                          'end_min': int})


def is_overlap(slot1: TimeSlotInfo, slot2: TimeSlotInfo) -> None or tuple[str, str]:
    """
    Given two time slots, this function returns None if they do not overlap, or a tuple of the
    start and end time of the overlap if they do.
    Note: The start and end time are formatted as HH:MM
    """
    slot1_start_time = slot1['start_hr']*60+slot1['start_min']
    slot1_end_time = slot1['end_hr']*60+slot1['end_min']
    slot2_start_time = slot2['start_hr']*60+slot2['start_min']
    slot2_end_time = slot2['end_hr']*60+slot2['end_min']

    if slot1_start_time <= slot2_start_time and slot2_start_time < slot1_end_time:
        start = slot2_start_time
        end = min(slot1_end_time, slot2_end_time)

        start_hours = int(start/60)
        start_min = start - (start_hours*60)

        end_hours = int(end/60)
        end_min = end - (end_hours*60)

        return (f"{int(start_hours):02d}:{int(start_min):02d}", f"{int(end_hours):02d}:{int(end_min):02d}")
    
    elif slot1_start_time < slot2_end_time and slot1_start_time >= slot2_start_time:
        start = slot1_start_time
        end = min(slot1_end_time, slot2_end_time)

        start_hours = int(start/60)
        start_min = start - (start_hours*60)

        end_hours = int(end/60)
        end_min = end - (end_hours*60)

        return (f"{int(start_hours):02d}:{int(start_min):02d}", f"{int(end_hours):02d}:{int(end_min):02d}")
   
    else: 
        return None

def course_overlap():
    """
    Function to create table and export a csv file
    """
    create_overlapping_sections_table_if_not_exists()

    with DatabaseConnection() as cursor:
        cursor.execute("select * from overlapping_sections;")
        results = cursor.fetchall()
        print(results)

        list_results = []
        for row in results:
            list_results.append(row)
        
        header=["day", "course_id_1", "sec_id_1", "year_1", "semester_1", "course_id_2", "sec_id_2", "year_2", "sem_2", "overlap_time_start", "overlap_time_end"]
        write_results_to_csv(header, list_results, 'task2.csv')

if __name__ == '__main__':
    course_overlap()
