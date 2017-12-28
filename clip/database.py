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

        # full caches
        self.institutions = {}  # [internal_id] -> {id, name}
        self.degrees = {}  # [internal_id] -> {id, name}
        self.periods = {}  # [letter][stage] -> db id
        self.departments = {}  # [internal_id] -> {id, name, initial_year, last_year}
        self.courses = {}  # [internal_id] -> {id, start, end}
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
        self.cursor.execute('SELECT internal_id, id, short_name, initial_year, last_year '
                            'FROM Institutions')
        for institution in self.cursor:
            institutions[institution[0]] = {
                'id': institution[1],
                'name': institution[2],
                'initial_year': institution[3],
                'last_year': institution[4]}
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
            if period[0] not in self.periods:  # unseen letter
                periods[period[0]] = {}

            if period[1] not in self.periods[period[0]]:  # unseen stage
                periods[period[1]] = {}

            periods[period[1]][period[0]] = period[2]
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
            departments[department[0]] = {
                'id': department[1],
                'name': department[2],
                'initial_year': department[3],
                'last_year': department[4],
                'institution': department[5]}
        self.lock.release()
        self.departments = departments

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

    # Reconstructs a turn instances, IT'S DESTRUCTIVE!
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
