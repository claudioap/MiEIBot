import sqlite3
import threading
import os
import re
from datetime import datetime
from queue import Queue

from clip.entities import Institution, Department, Course


def escape(string):
    return str(re.sub(re.compile("[^\w\d\s.-]", re.UNICODE), "", string))


class Database:
    def __init__(self, file=os.path.dirname(__file__) + '/CLIP.db'):
        self.link = sqlite3.connect(file, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self.cursor = self.link.cursor()
        self.lock = threading.Lock()

        # full caches
        self.institutions = {}  # [clip_id] -> Institution
        self.departments = {}  # [clip_id] -> Department
        self.degrees = {}  # [clip_id] -> {id, name}
        self.periods = {}  # [letter][stage] -> db id
        self.courses = {}  # [internal_id] -> Course
        self.course_abbreviations = {}  # [abbr] -> [internal_ids, ...]
        self.weekdays = {}  # [portuguese_name] -> internal_id
        self.turn_types = {}  # [abbr] -> db_id
        self.teachers = {}  # [name] -> db_id

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
        self.__load_weekdays__()
        self.__load_turn_types__()
        self.__load_teachers__()

    def __load_institutions__(self):  # build institution cache
        institutions = {}
        self.lock.acquire()
        self.cursor.execute('SELECT internal_id, id, abbreviation, name, initial_year, last_year '
                            'FROM Institutions')
        for institution in self.cursor:
            institutions[institution[0]] = Institution(
                institution[0], institution[2], name=institution[3],
                initial_year=institution[4], last_year=institution[5], db_id=institution[1])
        self.lock.release()
        self.institutions = institutions

    def __load_degrees__(self):  # build degree cache
        degrees = {}
        self.lock.acquire()
        self.cursor.execute('SELECT internal_id, id, name_en '
                            'FROM Degrees '
                            'WHERE internal_id NOT NULL')
        for degree in self.cursor:
            degrees[degree[0]] = {'id': degree[1], 'name': degree[2]}
        self.lock.release()
        self.degrees = degrees

    def __load_periods__(self):  # build period cache
        periods = {}
        self.lock.acquire()
        self.cursor.execute('SELECT type_letter, stage, id '
                            'FROM Periods')
        for period in self.cursor:
            if period[0] not in periods:  # unseen letter
                periods[period[0]] = {}

            periods[period[0]][period[1]] = period[2]
        self.lock.release()
        self.periods = periods

    def __load_departments__(self):  # build institution cache
        departments = {}
        self.lock.acquire()
        self.cursor.execute('SELECT Departments.internal_id, Departments.id, Departments.name, '
                            'Departments.initial_year, Departments.last_year, Institutions.internal_id '
                            'FROM Departments '
                            'JOIN Institutions ON Departments.institution = Institutions.id')
        for department in self.cursor:
            departments[department[0]] = Department(
                department[0], department[2], department[5],
                initial_year=department[3], last_year=department[4], db_id=department[1])

        self.lock.release()
        self.departments = departments

    def __load_courses__(self):  # build course abbr cache
        self.courses.clear()
        self.course_abbreviations.clear()

        self.lock.acquire()
        self.cursor.execute('SELECT internal_id, name, abbreviation, id, initial_year, last_year, degree, institution '
                            'FROM Courses '
                            'WHERE abbreviation IS NOT NULL')
        for course in self.cursor:
            course_obj = Course(course[0], course[1], course[2], course[6], course[7],
                                initial_year=course[4], last_year=course[5], db_id=course[3])
            self.courses[course[0]] = course_obj

            if course[2] not in self.course_abbreviations:
                self.course_abbreviations[course[2]] = []
            self.course_abbreviations[course[2]].append(course_obj)
        self.lock.release()

    def __load_weekdays__(self):  # build course abbr cache
        weekdays = {}
        self.lock.acquire()
        self.cursor.execute('SELECT id, name_pt '
                            'FROM Weekdays')
        for weekday in self.cursor:
            weekdays[weekday[1]] = weekday[0]
        self.lock.release()
        self.weekdays = weekdays

    def __load_turn_types__(self):  # build turn type cache
        turn_types = {}
        self.lock.acquire()
        self.cursor.execute('SELECT abbr, id, type '
                            'FROM TurnTypes')
        for turn_type in self.cursor:
            turn_types[turn_type[0]] = {'id': turn_type[1], 'name': turn_type[2]}
        self.lock.release()
        self.turn_types = turn_types

    def __load_teachers__(self):  # build teacher cache
        teachers = {}
        self.lock.acquire()
        self.cursor.execute('SELECT id, name '
                            'FROM Teachers')
        for teacher in self.cursor:
            teachers[teacher[1]] = teacher[0]
        self.lock.release()
        self.teachers = teachers

    def add_institutions(self, institutions):
        self.lock.acquire()
        try:
            for institution in institutions:
                if institution.identifier not in self.institutions:
                    self.cursor.execute(
                        'INSERT INTO Institutions(internal_id, abbreviation, name, initial_year, last_year) '
                        'VALUES (?, ?, ?, ?, ?)',
                        (institution.identifier, institution.abbreviation,
                         institution.name if institution.name != institution.abbreviation else None,
                         institution.initial_year, institution.last_year))

                else:
                    self.cursor.execute(
                        'SELECT name, last_year '
                        'FROM Institutions '
                        'WHERE internal_id =? AND abbreviation=?',
                        (institution.identifier, institution.abbreviation))
                    stored = self.cursor.fetchone()

                    # new name (could be previously unknown)
                    if institution.name is not None and institution.name != institution.abbreviation \
                            and institution.name != stored[0]:
                        self.cursor.execute(
                            'UPDATE Institutions '
                            'SET name=? '
                            'WHERE internal_id =? AND abbreviation=?',
                            (institution.name, institution.identifier, institution.abbreviation))

                    # new last year (new academic year)
                    if institution.last_year is not None and institution.last_year != stored[1]:
                        self.cursor.execute(
                            'UPDATE Institutions '
                            'SET last_year=? '
                            'WHERE internal_id =? AND abbreviation=?',
                            (institution.last_year, institution.identifier, institution.abbreviation))
            self.link.commit()
        finally:
            self.lock.release()
        print("Institutions added successfully!")
        self.__load_institutions__()

    def add_departments(self, departments):
        for department in departments:
            exists = False
            different = False

            if department.identifier in self.departments:
                stored_department = self.departments[department.identifier]
                exists = True
                if stored_department.name != department.name:
                    raise Exception("Different departments had an id collision:\n\tStored: {}\n\tNew: {}".format(
                        stored_department, departments))

                # creation date different or  last year different
                if department.initial_year != stored_department.initial_year \
                        or department.last_year != stored_department.last_year:
                    different = True

            if not exists:  # unknown department, add it to the DB
                print("Adding department {}".format(department))
                self.lock.acquire()
                try:
                    self.cursor.execute(
                        'INSERT INTO Departments(internal_id, name, initial_year, last_year, institution) '
                        'VALUES (?, ?, ?, ?, ?)',
                        (department.identifier, department.name, department.initial_year, department.last_year,
                         self.institutions[department.institution].db_id))
                finally:
                    self.lock.release()

            if different:  # department changed
                print("Updating department from:'{}' to:'{}'".format(stored_department, department))
                self.lock.acquire()
                try:
                    self.cursor.execute(
                        'UPDATE Departments '
                        'SET initial_year=?, last_year=? '
                        'WHERE internal_id=? AND institution=?',
                        (department.initial_year, department.last_year, department.identifier,
                         self.institutions[department.institution].db_id))
                finally:
                    self.lock.release()

        self.lock.acquire()
        try:
            self.link.commit()
        finally:
            self.lock.release()

        print("Departments added successfully!")
        self.__load_departments__()

    def add_class(self, class_, commit=False):
        if class_.department not in self.departments:
            raise Exception("Unknown department")

        department = self.departments[class_.department]

        exists = False
        class_db_id = None

        self.lock.acquire()
        try:
            self.cursor.execute('SELECT id, name '
                                'FROM Classes '
                                'WHERE internal_id=? AND department=?',
                                (class_.identifier, department.db_id))
            stored_classes = self.cursor.fetchall()
        finally:
            self.lock.release()

        for stored_class in stored_classes:
            stored_class_name = stored_class[1]
            if stored_class_name != class_.name:
                raise Exception("Id collision or class name change attempt. {} to {} (id was {})".format(
                    stored_class_name, class_.name, class_.identifier
                ))
            else:
                print("Already known: {}".format(class_))  # TODO Proper logging
                exists = True
                class_db_id = stored_class[0]

        if not exists:
            self.lock.acquire()
            try:
                print("Adding class {}".format(class_))
                self.cursor.execute('INSERT INTO Classes(internal_id, name, department) '
                                    'VALUES (?, ?, ?)',
                                    (class_.identifier, class_.name, department.db_id))

                # fetch the new id
                self.cursor.execute('SELECT id '
                                    'FROM Classes '
                                    'WHERE internal_id=? AND department=?',
                                    (class_.identifier, department.db_id))

                if commit:
                    self.link.commit()

                return self.cursor.fetchone()[0]
            finally:
                self.lock.release()

        if commit:
            self.lock.acquire()
            try:
                self.link.commit()
            finally:
                self.lock.release()

        return class_db_id

    def add_class_instances(self, instances):  # instances: [class_iid] ->[{year, period}, ...]

        # add class instance to the database
        for instance in instances:
            if instance.class_db_id is None:
                # figure out if the class id is cached
                if instance.class_id not in self.class_id_cache:  # fetch it
                    self.lock.acquire()
                    try:
                        self.cursor.execute('SELECT id '
                                            'FROM Classes '
                                            'WHERE internal_id=?',
                                            (instance.class_id,))
                        result = self.cursor.fetchone()
                    finally:
                        self.lock.release()

                    if result is None:
                        raise Exception("Unable to find the parent class of the instance {}".format(instance))

                    self.class_id_cache[instance.class_id] = result[0]

                class_db_id = self.class_id_cache[instance.class_id]  # obtain the class id from the cache
            else:
                class_db_id = instance.class_db_id

            self.lock.acquire()
            try:
                self.cursor.execute('SELECT period, year '
                                    'FROM ClassInstances '
                                    'WHERE class=?',
                                    (class_db_id,))

                exists = False

                for stored_instance in self.cursor.fetchall():  # for every instance matching this instance parent class
                    if stored_instance[0] == instance.period and stored_instance[1] == instance.year:
                        exists = True
                        break

                if not exists:  # unknown class instance, add it to the DB
                    print("Adding instance of {}".format(instance))
                    self.cursor.execute('INSERT INTO ClassInstances(class, period, year) '
                                        'VALUES (?, ?, ?)',
                                        (class_db_id, instance.period, instance.year))
            finally:
                self.lock.release()

        self.lock.acquire()
        try:
            self.link.commit()
        finally:
            self.lock.release()

        print("Class instances added successfully!")

    def add_courses(self, courses):
        for course in courses:
            exists = False
            different_time = False
            different_abbreviation = False
            different_degree = False

            self.lock.acquire()
            try:
                self.cursor.execute('SELECT name, initial_year, last_year, abbreviation, degree '
                                    'FROM Courses '
                                    'WHERE institution=? AND internal_id=?',
                                    (self.institutions[course.institution].db_id, course.identifier))
                stored_courses = self.cursor.fetchall()
            finally:
                self.lock.release()

            for stored_course in stored_courses:
                stored_course_name = course[0]
                stored_course_initial_year = course[1]
                stored_course_last_year = course[2]
                stored_course_abbreviation = course[3]
                stored_course_degree = course[4]

                if course.name != stored_course_name:
                    raise Exception("Different courses had an id collision {} with {}".format(courses, stored_course))

                exists = True

                # course information changed (or previously unknown information appeared)
                if stored_course_initial_year != course.initial_year or stored_course_last_year != course.last_year:
                    different_time = True

                if stored_course_abbreviation != course.abbreviation:
                    different_abbreviation = True

                if stored_course_degree != course.degree:
                    different_degree = True

            if not exists:  # unknown department, add it to the DB
                print("Adding course: {}".format(course))
                self.lock.acquire()
                try:
                    self.cursor.execute(
                        'INSERT INTO Courses'
                        '(internal_id, name, initial_year, last_year, abbreviation, degree, institution) '
                        'VALUES (?, ?, ?, ?, ?, ?, ?)',
                        (course.identifier, course.name, course.initial_year, course.last_year,
                         course.abbreviation, course.degree, self.institutions[course.institution].identifier))
                finally:
                    self.lock.release()

            if different_time:  # update running date
                if course.initial_year is not None and course.last_year is not None:
                    print("Updating course {}({}) (now goes from {} to {})".format(
                        course.name, course.identifier, course.initial_year, course.last_year))
                    self.lock.acquire()
                    try:
                        self.cursor.execute(
                            'UPDATE Courses '
                            'SET initial_year = ?, last_year=? '
                            'WHERE internal_id=? AND institution=?',
                            (course.initial_year,
                             course.last_year,
                             course.identifier,
                             self.institutions[course.institution].identifier))
                    finally:
                        self.lock.release()

            if different_abbreviation:  # update abbreviation
                if course.abbreviation is not None:
                    print("Updating course {}({}) abbreviation to {}".format(
                        course.name, course.identifier, course.abbreviation))
                    self.lock.acquire()
                    try:
                        self.cursor.execute(
                            'UPDATE Courses '
                            'SET abbreviation=? '
                            'WHERE internal_id=? AND institution=?',
                            (course.abbreviation, course.identifier, self.institutions[course.institution].identifier))
                    finally:
                        self.lock.release()

            if different_degree:  # update degree
                if course.degree is not None:
                    print("Updating course {}({}) degree to {}".format(
                        course.name, course.identifier, course.degree))
                    self.lock.aqcuire()
                    try:
                        self.cursor.execute(
                            'UPDATE Courses '
                            'SET degree=? '
                            'WHERE internal_id=? AND institution=?',
                            (course.degree, course.identifier, self.institutions[course.institution].identifier))
                    finally:
                        self.lock.release()

        self.lock.acquire()
        try:
            self.link.commit()
            print("Courses added successfully!")
        finally:
            self.lock.release()
        self.__load_courses__()

    # adds/updates the student information. Returns student id. DOES NOT COMMIT BY DEFAULT
    def add_student(self, student_iid, name, course=None, abbr=None, institution=None, commit=False):
        course_id = None if course is None else self.courses[course].identifier
        institution_id = None if institution is None else self.institutions[institution].identifier
        abbr = abbr if abbr != '' else None

        if name is None or name == '':
            raise Exception("Invalid name")

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
                                    (name, student_iid, abbr, course, institution_id))
                print("New student saved {}({}-{})".format(name, student_iid, abbr))  # TODO proper logging
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
                    print("Updated student info {}({}-{})".format(name, student_iid, abbr))  # TODO proper logging
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

    def add_turn(self, class_instance, number, turn_type, restrictions=None, weekly_hours=None,
                 enrolled=None, teachers=None, routes=None, capacity=None, state=None, commit=False):
        self.lock.acquire()
        self.cursor.execute(
            'SELECT id, restrictions, hours, enrolled, capacity, routes, state '
            'FROM Turns '
            'WHERE class_instance=? AND number=? AND type=?')
        turns = self.cursor.fetchall()
        self.lock.release()

        turn_id = self.turn_types[turn_type]

        if len(turns) > 1:
            raise Exception("We've got a consistency problem... Turn {}.{} of {}\nMatches:{}".format(
                turn_type, number, class_instance, str(turns)))

        if len(turns) == 0:
            self.lock.acquire()
            try:
                self.cursor.execute(
                    'INSERT INTO Turns'
                    '(class_instance, number, type, restrictions, hours, enrolled, routes, capacity, state) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (class_instance, number, turn_id, restrictions, weekly_hours, enrolled, routes, capacity, state))
            finally:
                self.lock.release()

        old_turn_info = turns[0]

        different = False
        turn_id = old_turn_info[0]
        new_restrictions = old_turn_info[1]
        new_hours = old_turn_info[2]
        new_enrolled = old_turn_info[3]
        new_capacity = old_turn_info[4]
        new_routes = old_turn_info[5]
        new_state = old_turn_info[6]

        if restrictions is not None and restrictions is not '':
            new_restrictions = restrictions
            different = True

        if weekly_hours is not None:
            new_hours = weekly_hours
            different = True

        if enrolled is not None:
            new_enrolled = enrolled
            different = True

        if capacity is not None:
            new_capacity = capacity
            different = True

        if routes is not None:
            new_routes = routes
            different = True

        if state is not None:
            new_state = state
            different = True

        if different:
            self.lock.acquire()
            try:
                self.cursor.execute(
                    'UPDATE Turns '
                    'SET restrictions=?, hours=?, enrolled=?, capacity=?, routes=?, state=? '
                    'WHERE id=?',
                    (new_restrictions, new_hours, new_enrolled, new_capacity, new_routes, new_state, turn_id))
            finally:
                self.lock.release()

        if teachers is not None:
            for teacher in teachers:
                if teacher not in self.teachers:
                    self.lock.acquire()
                    try:
                        self.cursor.execute(
                            'INSERT INTO Teachers(name) '
                            'VALUES (?)', (teacher,))
                    finally:
                        self.lock.release()
                    self.__load_teachers__()  # lock needs to release, otherwise this would wait forever

                self.lock.acquire()
                try:
                    self.cursor.execute(
                        'SELECT 1 FROM TurnTeachers '
                        'WHERE turn=? AND teacher=?',
                        (turn_id, self.teachers[teacher]))
                    if self.cursor.fetchone() is None:
                        self.cursor.execute(
                            'INSERT INTO TurnTeachers(turn, teacher) '
                            'VALUES (?, ?)',
                            (turn_id, self.teachers[teacher]))
                finally:
                    self.lock.release()

        if commit:
            self.lock.acquire()
            try:
                self.link.commit()
            finally:
                self.lock.release()

        return turn_id

    # Reconstructs the instances of a turn , IT'S DESTRUCTIVE!
    def add_turn_instances(self, turn_id, instances):
        self.lock.acquire()
        try:
            self.cursor.execute(
                'DELETE FROM TurnInstances '
                'WHERE turn=?', (turn_id,))
            db_instance = self.cursor.fetchone()
            for instance in instances:
                # FIXME Classrooms and Buildings
                self.cursor.execute(
                    'INSERT INTO TurnInstances(turn, start, end, weekday, classroom) '
                    'VALUES (?, ?, ?, ?, ?)',
                    (turn_id, instance['start'], instance['end'], instance['weekday'], None))

            self.link.commit()
        finally:
            self.lock.release()

    def add_turn_students(self, turn_id, students):
        self.lock.acquire()
        try:
            for student in students:
                self.cursor.execute(
                    'SELECT 1 '
                    'FROM TurnStudents '
                    'WHERE turn=? AND student=?',
                    (turn_id, student))
                if self.cursor.fetchone() is None:
                    self.cursor.execute(
                        'INSERT INTO TurnStudents(turn, student) '
                        'VALUES (?, ?)',
                        (turn_id, student))
            self.link.commit()
        finally:
            self.lock.release()

    def add_admissions(self, admissions):
        for admission in admissions:
            self.lock.acquire()
            try:
                self.cursor.execute(
                    'INSERT INTO Admissions '
                    '(student, name, course, phase, year, option, state, check_date) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (admission.student_id, self.courses[admission.course].db_id, admission.phase, admission.year,
                     admission.option, admission.state, admission.check_date, admission.name))
                self.link.commit()
            finally:
                self.lock.release()

    def add_enrollments(self, enrollments):
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

    def fetch_class_instances(self, year_asc=True):
        class_instances = Queue()
        self.lock.acquire()
        try:
            if year_asc:
                self.cursor.execute('SELECT class_instance_id, class_iid, period_stage, period_letter, '
                                    'year, department_iid, institution_iid '
                                    'FROM ClassInstancesComplete '
                                    'ORDER BY year ASC')
            else:
                self.cursor.execute('SELECT class_instance_id, class_iid, period_stage, period_letter, '
                                    'year, department_iid, institution_iid '
                                    'FROM ClassInstancesComplete '
                                    'ORDER BY year DESC')

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
            self.lock.release()
        return class_instances

    def commit(self):
        self.lock.acquire()
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
