import sqlite3
import threading
import os
import re
from datetime import datetime
from queue import Queue


def escape(string):
    return str(re.sub(re.compile("[^\w\d\s.-]", re.UNICODE), "", string))


class Database:
    def __init__(self, file=os.path.dirname(__file__) + '/CLIP.db'):
        self.link = sqlite3.connect(file, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self.cursor = self.link.cursor()
        self.lock = threading.Lock()

        # fully caches
        self.institutions = {}  # [internal_id] -> {id, name}
        self.degrees = {}  # [internal_id] -> {id, name}
        self.periods = {}  # [letter][stage] -> db id
        self.departments = {}  # [internal_id] -> {id, name, initial_year, last_year}
        self.courses = {}  # [internal_id] -> {id, start, end}
        self.course_abbreviations = {}  # [abbr] -> [internal_ids, ...]

        # partial caches (built as requests are made)
        self.class_id_cache = {}
        self.class_department_cache = {}
        self.course_id_cache = {}

        self.__load_cached_collections__()

    def __load_cached_collections__(self):
        self.__load_institutions__()
        self.__load_degrees__()
        self.__load_periods__()
        self.__load_departments__()
        self.__load_courses__()

    def __load_institutions__(self):  # build institution cache
        self.lock.acquire()
        self.cursor.execute('SELECT internal_id, id, short_name, initial_year, last_year '
                            'FROM Institutions')
        for institution in self.cursor:
            self.institutions[institution[0]] = {
                'id': institution[1],
                'name': institution[2],
                'initial_year': institution[3],
                'last_year': institution[4]}
        self.lock.release()

    def __load_degrees__(self):  # build degree cache
        self.lock.acquire()
        self.cursor.execute('SELECT internal_id, id, name_en '
                            'FROM Degrees '
                            'WHERE internal_id NOT NULL')
        for degree in self.cursor:
            self.degrees[degree[0]] = {'id': degree[1], 'name': degree[2]}
        self.lock.release()

    def __load_periods__(self):  # build period cache
        self.lock.acquire()
        self.cursor.execute('SELECT type_letter, stage, id '
                            'FROM Periods')
        for period in self.cursor:
            if period[0] not in self.periods:  # unseen letter
                self.periods[period[0]] = {}

            if period[1] not in self.periods[period[0]]:  # unseen stage
                self.periods[period[1]] = {}

            self.periods[period[1]][period[0]] = period[2]
        self.lock.release()

    def __load_departments__(self):  # build institution cache
        self.lock.acquire()
        self.cursor.execute('SELECT Departments.internal_id, Departments.id, Departments.name, '
                            'Departments.initial_year, Departments.last_year, Institutions.internal_id '
                            'FROM Departments '
                            'JOIN Institutions ON Departments.institution = Institutions.id')
        for department in self.cursor:
            self.departments[department[0]] = {
                'id': department[1],
                'name': department[2],
                'initial_year': department[3],
                'last_year': department[4],
                'institution': department[5]}
        self.lock.release()

    def __load_courses__(self):  # build course abbr cache
        self.courses.clear()
        self.course_abbreviations.clear()

        self.lock.acquire()
        self.cursor.execute('SELECT internal_id, abbreviation, id, initial_year, final_year '
                            'FROM Courses '
                            'WHERE abbreviation IS NOT NULL')
        for course in self.cursor:
            self.courses[course[0]] = {
                'id': course[2],
                'start': course[3],
                'end': course[4]
            }

            if course[1] not in self.course_abbreviations:
                self.course_abbreviations[course[1]] = []
            self.course_abbreviations[course[1]].append(course[0])
        self.lock.release()

    def add_departments(self, departments):
        institutions = {}  # cache
        for department_iid, department_info in departments.items():
            institution_iid = department_info['institution']
            if institution_iid not in institutions:
                institutions[department_info['institution']] = self.institutions[institution_iid]['id']
            institution_id = institutions[institution_iid]

            exists = False
            different = False

            self.lock.acquire()
            try:
                self.cursor.execute('SELECT name, initial_year, last_year '
                                    'FROM Departments '
                                    'WHERE institution=? AND internal_id=?',
                                    (institution_id, department_iid))
                for department in self.cursor.fetchall():
                    if department[0] != department_info['name']:
                        raise Exception("Different departments had an iid collision, {}. ({} != {})".format(
                            department_iid, department[0], department_info['name']
                        ))

                    # creation date different or  last year different
                    elif department[1] != department_info['initial_year'] \
                            or department[2] != department_info['last_year']:
                        different = True
                        exists = True

                if not exists:  # unknown department, add it to the DB
                    print("Adding department '{}'({}) {} - {} to the database".format(
                        department_info['name'],
                        department_iid,
                        department_info['initial_year'],
                        department_info['last_year']))
                    self.cursor.execute(
                        'INSERT INTO Departments(internal_id, name, initial_year, last_year, institution) '
                        'VALUES (?, ?, ?, ?, ?)',
                        (department_iid,
                         department_info['name'],
                         department_info['initial_year'],
                         department_info['last_year'],
                         institution_id))

                if different:  # department changed
                    print("Updating department '{}'() (now goes from {} to {})".format(
                        department_info['name'],
                        department_iid,
                        department_info['creation'],
                        department_info['last_year']))
                    self.cursor.execute(
                        'UPDATE Departments '
                        'SET initial_year=?, last_year=? '
                        'WHERE internal_id=? AND institution=?',
                        (department_info['creation'],
                         department_info['last_year'],
                         department_iid,
                         institution_id))
            finally:
                self.lock.release()

        self.lock.acquire()
        try:
            self.link.commit()
            print("Changes saved")
        finally:
            self.lock.release()

        self.__load_departments__()

    def add_classes(self, classes):
        departments = {}  # cache
        for class_iid, class_info in classes.items():
            department_iid = class_info['department']
            if department_iid not in departments:
                departments[class_info['institution']] = self.departments[department_iid]['id']
            department_id = departments[department_iid]

            if class_iid not in self.class_department_cache:  # cache this department TODO useful?
                self.class_department_cache[class_iid] = department_id

            exists = False

            self.lock.acquire()
            try:
                self.cursor.execute('SELECT name '
                                    'FROM Classes '
                                    'WHERE internal_id=? AND department=?',
                                    (class_iid, department_id))
                classes = self.cursor.fetchall()
            finally:
                self.lock.release()

            for class_ in classes:
                if class_[0] != class_info['name']:
                    raise Exception("Different classes had an iid collision {} with {}, iid was {}".format(
                        class_info['name'], class_[0], class_iid
                    ))
                else:
                    print("Class already known: {}({})".format(class_info['name'], class_iid))  # TODO Proper logging
                    exists = True

            if not exists:
                self.lock.acquire()
                try:
                    print("Adding  {}({}) to the database".format(class_info['name'], class_iid))
                    self.cursor.execute('INSERT INTO Classes(internal_id, name, department) VALUES (?, ?, ?)',
                                        (class_iid, class_info['name'], class_info['department']))
                finally:
                    self.lock.release()

        self.lock.acquire()
        try:
            self.link.commit()
            print("Changes saved")
        finally:
            self.lock.release()

    def add_class_instances(self, instances):  # instances: [class_iid] ->[{year, period}, ...]

        # add class instance to the database
        for class_iid, instance_info in instances.items():
            # figure out if the class id is unknown

            if class_iid not in self.class_id_cache:  # fetch it
                self.lock.acquire()
                try:
                    self.cursor.execute('SELECT id '
                                        'FROM Classes '
                                        'WHERE internal_id=?',
                                        (class_iid,))
                    result = self.cursor.fetchone()
                finally:
                    self.lock.release()

                if result is None:
                    raise Exception("Unknown class id for {}, info: {}".format(class_iid, instances))

                self.class_id_cache[class_iid] = result[0]

            # obtain the class id from the cache
            class_id = self.class_id_cache[class_iid]
            self.lock.acquire()
            try:
                for instance in instance_info:
                    self.cursor.execute('SELECT period, year '
                                        'FROM ClassInstances '
                                        'WHERE class=?',
                                        (class_id,))

                    exists = False

                    for row in self.cursor.fetchall():  # for every class matching the iid
                        if row[0] == instance['period'] and row[1] == instance['year']:
                            exists = True
                            break

                    if not exists:  # unknown class instance, add it to the DB
                        print("Adding class iid {} instance in period {} of {} to the database".format(  # TODO logging
                            class_iid, instance['period'], instance['year']))
                        self.cursor.execute('INSERT INTO ClassInstances(class, period, year) '
                                            'VALUES (?, ?, ?)',
                                            (class_id, instance['period'], instance['year']))
            finally:
                self.lock.release()

        self.lock.aquire()
        try:
            self.link.commit()
            print("Changes saved")
        finally:
            self.lock.release()

    def add_courses(self, courses):
        for course_iid, course_info in courses.items():
            exists = False
            different_time = False
            different_abbreviation = False
            different_degree = False

            self.lock.acquire()
            try:
                self.cursor.execute('SELECT name, initial_year, final_year, abbreviation, degree '
                                    'FROM Courses '
                                    'WHERE institution=? AND internal_id=?',
                                    (course_info['institution']['id'], course_iid))
                courses = self.cursor.fetchall()
            finally:
                self.lock.release()

            for course in courses:
                course_name = course[0]
                course_initial_year = course[1]
                course_final_year = course[2]
                course_abbreviation = course[3]
                course_degree = course[4]

                if course_name != course_info['name']:
                    raise Exception("Different courses had an id collision")

                exists = True

                # course information changed (or previously unknown information appeared)
                if course_initial_year != course_info['initial_year'] or course_final_year != course_info['final_year']:
                    different_time = True

                if course_abbreviation != course_info['abbreviation']:
                    different_abbreviation = True

                if course_degree != course_info['degree']:
                    different_degree = True

            if not exists:  # unknown department, add it to the DB
                print("Adding  {}({}, {}) {} - {} to the database".format(
                    course_info['name'],
                    course_iid,
                    course_info['abbreviation'],
                    course_info['initial_year'],
                    course_info['final_year']))
                self.lock.aquire()
                try:
                    self.cursor.execute(
                        'INSERT INTO Courses'
                        '(internal_id, name, initial_year, final_year, abbreviation, degree, institution) '
                        'VALUES (?, ?, ?, ?, ?, ?, ?)',
                        (course_iid,
                         course_info['name'],
                         course_info['initial_year'],
                         course_info['final_year'],
                         course_info['abbreviation'],
                         course_info['degree'],
                         self.institutions[course_info['institution']]['id']))
                finally:
                    self.lock.release()

            if different_time:  # update running date
                if course_info['initial_year'] is not None and course_info['final_year'] is not None:
                    print("Updating course {}({}) (now goes from {} to {})".format(
                        course_info['name'], course_iid, course_info['initial_year'], course_info['final_year']))
                    self.lock.aquire()
                    try:
                        self.cursor.execute(
                            'UPDATE Courses '
                            'SET initial_year = ?, final_year=? '
                            'WHERE internal_id=? AND institution=?',
                            (course_info['initial_year'],
                             course_info['final_year'],
                             course_iid,
                             self.institutions[course_info['institution']]['id']))
                    finally:
                        self.lock.release()

            if different_abbreviation:  # update abbreviation
                if course_info['abbreviation'] is not None:
                    print("Updating course {}({}) abbreviation to {}".format(
                        course_info['name'], course_iid, course_info['abbreviation']))
                    self.lock.aquire()
                    try:
                        self.cursor.execute(
                            'UPDATE Courses '
                            'SET abbreviation=? '
                            'WHERE internal_id=? AND institution=?',
                            (course_info['abbreviation'],
                             course_iid, self.institutions[course_info['institution']]['id']))
                    finally:
                        self.lock.release()

            if different_degree:  # update degree
                if course_info['degree'] is not None:
                    print("Updating course {}({}) degree to {}".format(
                        course_info['name'], course_iid, course_info['degree']))
                    self.lock.aquire()
                    try:
                        self.cursor.execute(
                            'UPDATE Courses '
                            'SET degree=? '
                            'WHERE internal_id=? AND institution=?',
                            (course_info['degree'], course_iid,
                             self.institutions[course_info['institution']]['id']))
                    finally:
                        self.lock.release()

        self.lock.aquire()
        try:
            self.link.commit()
            print("Changes saved")
        finally:
            self.lock.release()
        self.__load_courses__()

    # adds/updates the student information. Returns student id. DOES NOT COMMIT BY DEFAULT
    def add_student(self, student_iid, name, course=None, abbr=None, institution=None, commit=False):
        course_id = None if course is None else self.courses[course]['id']
        institution_id = None if institution is None else self.institutions[institution]['id']

        self.lock.acquire()
        try:
            if institution_id is None:
                self.cursor.execute('SELECT id, abbreviation, course, institution '
                                    'FROM Students '
                                    'WHERE internal_id=? AND name=?',
                                    (student_iid, name))
            else:
                self.cursor.execute('SELECT id, abbreviation, course, institution '
                                    'FROM Students '
                                    'WHERE internal_id=? AND name=? AND institution=?',
                                    (student_iid, name, institution_id))
            matching_students = self.cursor.fetchall()
        finally:
            self.lock.release()

        registered = False
        if len(matching_students) == 0:  # new student, add him
            self.lock.acquire()
            try:
                self.cursor.execute('INSERT INTO STUDENTS(name, internal_id, abbreviation, course, institution) '
                                    'VALUES (?, ?, ?, ?, ?)',
                                    (name,
                                     student_iid,
                                     abbr,
                                     course,
                                     institution_id))
                print("New student saved")  # TODO proper logging
            finally:
                self.lock.release()
        elif len(matching_students) == 1:
            registered = True
        else:  # bug or several institutions (don't know if it is even possible)
            raise Exception("Duplicated students found. {}({})".format(student_iid, name))

        if registered:
            stored_id = matching_students[0][0]
            stored_abbr = matching_students[0][1]
            stored_course = matching_students[0][2]
            stored_institution = matching_students[0][3]

            abbr = None if abbr.strip() == '' else abbr
            if abbr is not None and stored_abbr is not None and stored_abbr != abbr.strip():
                raise Exception("Abbreviation mismatch. {} != {}".format(abbr, stored_abbr))

            if stored_abbr != abbr or stored_course != course_id or stored_institution != institution_id:
                new_abbr = stored_abbr if abbr is None else abbr
                new_institution = stored_institution if institution_id is None else institution_id
                new_course = stored_course if course_id is None else course_id

                self.lock.acquire()
                try:
                    self.cursor.execute("UPDATE STUDENTS "
                                        "SET abbreviation=?, institution=?, course=? "
                                        "WHERE id=?",
                                        (new_abbr, new_institution, new_course, stored_id))
                    print("Updated student info")  # TODO proper logging
                    if commit:
                        self.link.commit()
                finally:
                    self.lock.release()
                return stored_id

        # new student, fetch the new id
        self.lock.acquire()
        try:
            self.cursor.execute('SELECT id '
                                'FROM Students '
                                'WHERE internal_id=? AND name=?',
                                (student_iid, name))
            if commit:
                self.link.commit()
            return self.cursor.fetchone()[0]
        finally:
            self.lock.release()

    def add_admissions(self, admissions):
        for admission in admissions:
            self.lock.acquire()
            try:
                self.cursor.execute(
                    'INSERT INTO Admissions '
                    '(student_id, course_id, phase, year, option, state, check_date, name) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (admission['student_id'],
                     admission['course'],
                     admission['phase'],
                     admission['year'],
                     admission['option'],
                     admission['state'],
                     datetime.now(),
                     admission['name'] if admission['student'] is None else None))
                self.link.commit()
            finally:
                self.lock.release()

    def add_enrollements(self, enrollments):
        self.lock.acquire()
        try:
            for enrollment in enrollments:
                try:
                    self.cursor.execute(
                        'INSERT INTO  Enrollments'
                        '(student_id, class_instance, attempt, student_year, statutes, observation) '
                        'VALUES (?, ?, ?, ?, ?, ?)',
                        (enrollment['student_id'],
                         enrollment['class_instance'],
                         enrollment['attempt'],
                         enrollment['student_year'],
                         enrollment['statutes'],
                         enrollment['observation']))
                except sqlite3.Error:
                    print("Row existed already (I hope so)")  # FIXME There are certainly better ways
        finally:
            self.lock.release()

    def fetch_class_instances(self):
        class_instances = Queue()
        self.lock.acquire()
        try:
            self.cursor.execute('SELECT class_instance_id, class_iid, period_stage, period_letter, '
                                'year, department_iid, institution_iid '
                                'FROM ClassInstancesComplete '
                                'ORDER BY year ASC')

            for instance in self.cursor.fetchall():
                class_instances.put({
                    'class_instance': instance[0],
                    'class': instance[1],
                    'period': instance[2],
                    'period_type': instance[3],
                    'year': instance[4],
                    'department': instance[5],
                    'institution': instance[6]
                })
        finally:
            self.lock.acquire()
        return class_instances

    def commit(self):
        self.lock.aquire()
        try:
            self.link.commit()
        finally:
            self.lock.release()

    def find_student(self, name):
        nice_try = escape(name)
        query_string = '%'
        for word in nice_try.split():
            query_string += (word + '%')

        self.lock.acquire()
        try:
            self.cursor.execute("SELECT internal_id, name, abbreviation "
                                "FROM Students "
                                "WHERE name LIKE '{}'".format(query_string))
            return set(self.cursor.fetchall())
        finally:
            self.lock.release()