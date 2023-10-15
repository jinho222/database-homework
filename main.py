import psycopg
from psycopg import sql
import os
from typing import Union

SCHEMA = 'myschema'

# alias
S = 'students'
CR = 'course_registration'
C = 'course'
G = 'grade'
F = 'faculty'
L = 'lectureroom'
B = 'building'

# problem 1
def entire_search(CONNECTION: str, table_name: str) -> list:
    with psycopg.connect(CONNECTION) as conn:
        query = sql.SQL("""
            select *
            from {table} 
        """).format(
            table = sql.Identifier(SCHEMA, table_name),
        )
        return conn.execute(query).fetchall()


# problem 2
def registration_history(CONNECTION: str, student_id: str) -> Union[list, None]:
    with psycopg.connect(CONNECTION) as conn:
        query = sql.SQL("""
            select {C}.{YEAR},
              {C}.{SEMESTER},
              {C}.{COURSE_ID_PREFIX},
              {C}.{COURSE_ID_NO},
              {C}.{DIVISION_NO},
              {C}.{COURSE_NAME},
              {F}.{NAME},
              {G}.{GRADE}
            from {S}
                join {CR} on {S}.{STUDENT_ID} = {CR}.{STUDENT_ID}
                join {C} on {CR}.{COURSE_ID} = {C}.{COURSE_ID}
                join {G} on {C}.{COURSE_ID} = {G}.{COURSE_ID} and {S}.{STUDENT_ID} = {G}.{STUDENT_ID}
                join {F} on {C}.{PROF_ID} = {F}.{ID}
            where {S}.{STUDENT_ID} = %s 
            order by {C}.{YEAR},
              {C}.{SEMESTER},
              {C}.{COURSE_NAME}
        """).format(
            S = sql.Identifier(SCHEMA, S),
            CR = sql.Identifier(SCHEMA, CR),
            C = sql.Identifier(SCHEMA, C),
            G = sql.Identifier(SCHEMA, G),
            F = sql.Identifier(SCHEMA, F),
            STUDENT_ID = sql.Identifier('STUDENT_ID'),
            COURSE_ID = sql.Identifier('COURSE_ID'),
            PROF_ID = sql.Identifier('PROF_ID'),
            ID = sql.Identifier('ID'),
            YEAR = sql.Identifier('YEAR'),
            SEMESTER = sql.Identifier('SEMESTER'),
            COURSE_ID_PREFIX = sql.Identifier('COURSE_ID_PREFIX'),
            COURSE_ID_NO = sql.Identifier('COURSE_ID_NO'),
            DIVISION_NO = sql.Identifier('DIVISION_NO'),
            COURSE_NAME = sql.Identifier('COURSE_NAME'),
            NAME = sql.Identifier('NAME'),
            GRADE = sql.Identifier('GRADE')
        )

        executed =  conn.execute(query, (student_id, ))
        fetched = executed.fetchall()

        if len(fetched) == 0:
            print(f'Not Exist student with STUDENT ID: {student_id}')
            return None

        return fetched


# problem 3
def registration(CONNECTION: str, course_id: int, student_id: str) -> Union[list, None]:
    with psycopg.connect(CONNECTION) as conn:
        # check
        if not check_course_exists(conn, course_id):
            print(f"Not Exist course with COURSE ID: {course_id}")
            return None

        if not check_student_exists(conn, student_id):
            print(f"Not Exist student with STUDENT ID: {student_id}")
            return None

        if check_is_course_registered_by_student(conn, course_id, student_id):
            print(f"{student_id} is already registered in {course_id}")
            return None

        # update table
        query_insert = sql.SQL("""
            insert into {CR} 
            values (%s, %s)
        """).format(
            CR = sql.Identifier(SCHEMA, CR)
        )

        conn.execute(query_insert, (course_id, student_id))

        query_course_registration = sql.SQL("""
            select *
            from {CR}
        """).format(
            CR = sql.Identifier(SCHEMA, CR),
        )

        return conn.execute(query_course_registration).fetchall()


# problem 4
def withdrawal_registration(CONNECTION: str, course_id: int, student_id: str) -> Union[list, None]:
    with psycopg.connect(CONNECTION) as conn:
        # check
        if not check_course_exists(conn, course_id):
            print(f"Not Exist course with COURSE ID: {course_id}")
            return None

        if not check_student_exists(conn, student_id):
            print(f"Not Exist student with STUDENT ID: {student_id}")
            return None

        if not check_is_course_registered_by_student(conn, course_id, student_id):
            print(f"{student_id} is not registered in {course_id}")
            return None

        # update table
        query_delete = sql.SQL("""
            delete from {CR} 
            where {COURSE_ID} = %s and {STUDENT_ID} = %s
        """).format(
            CR = sql.Identifier(SCHEMA, CR),
            COURSE_ID = sql.Identifier('COURSE_ID'),
            STUDENT_ID=sql.Identifier('STUDENT_ID'),
        )

        conn.execute(query_delete, (course_id, student_id))

        query_course_registration = sql.SQL("""
            select *
            from {CR}
        """).format(
            CR = sql.Identifier(SCHEMA, CR),
        )

        return conn.execute(query_course_registration).fetchall()

# problem 5
def modify_lectureroom(CONNECTION: str, course_id: int, buildno: str, roomno: str) -> Union[list, None]:
    with psycopg.connect(CONNECTION) as conn:
        # check
        if not check_course_exists(conn, course_id):
            print(f"Not Exist course with COURSE ID: {course_id}")
            return None

        if not check_lecture_room_exists(conn, buildno, roomno):
            print(f"“Not Exist lecture room with BUILD NO: {buildno} / ROOM NO: {roomno}")
            return None

        # update table
        query_update = sql.SQL("""
            update {C}
            set {BUILDNO} = %s,
                {ROOMNO} = %s
            where {COURSE_ID} = %s
        """).format(
            C = sql.Identifier(SCHEMA, C),
            BUILDNO = sql.Identifier('BUILDNO'),
            ROOMNO=sql.Identifier('ROOMNO'),
            COURSE_ID=sql.Identifier('COURSE_ID'),
        )

        conn.execute(query_update, (buildno, roomno, course_id))

        query_course = sql.SQL("""
            select *
            from {C}
        """).format(
            C = sql.Identifier(SCHEMA, C),
        )

        return conn.execute(query_course).fetchall()


# sql file execute ( Not Edit )
def execute_sql(CONNECTION, path):
    folder_path = '/'.join(path.split('/')[:-1])
    file = path.split('/')[-1]
    if file in os.listdir(folder_path):
        with psycopg.connect(CONNECTION) as conn:
            conn.execute(open(path, 'r', encoding='utf-8').read())
            conn.commit()
        print("{} EXECUTRED!".format(file))
    else:
        print("{} File Not Exist in {}".format(file, folder_path))

def check_course_exists(conn, course_id):
    query = sql.SQL("""
        select *
        from {C}
        where {COURSE_ID} = %s
    """).format(
        C=sql.Identifier(SCHEMA, C),
        COURSE_ID=sql.Identifier('COURSE_ID')
    )

    fetched = conn.execute(query, (course_id,)).fetchall()

    return len(fetched) > 0

def check_student_exists(conn, student_id):
    query = sql.SQL("""
        select *
        from {S}
        where {STUDENT_ID} = %s
    """).format(
        S=sql.Identifier(SCHEMA, S),
        STUDENT_ID=sql.Identifier('STUDENT_ID')
    )

    fetched = conn.execute(query, (student_id,)).fetchall()

    return len(fetched) > 0

def check_is_course_registered_by_student(conn, course_id, student_id):
    query = sql.SQL("""
        select *
        from {CR}
        where {COURSE_ID} = %s
            and {STUDENT_ID} = %s
    """).format(
        CR=sql.Identifier(SCHEMA, CR),
        COURSE_ID=sql.Identifier('COURSE_ID'),
        STUDENT_ID=sql.Identifier('STUDENT_ID')
    )

    fetched = conn.execute(query, (course_id, student_id)).fetchall()

    return len(fetched) > 0

def check_lecture_room_exists(conn, build_no, room_no):
    query = sql.SQL("""
        select *
        from {L}
        where {BUILDNO} = %s and {ROOMNO} = %s
    """).format(
        L=sql.Identifier(SCHEMA, L),
        BUILDNO=sql.Identifier('BUILDNO'),
        ROOMNO = sql.Identifier('ROOMNO')
    )

    fetched = conn.execute(query, (build_no, room_no)).fetchall()

    return len(fetched) > 0
