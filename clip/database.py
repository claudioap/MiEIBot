import logging
import sqlite3
import threading
import os
import re
from queue import Queue

from clip.entities import Institution, Department, Course, Period, ClassInstance, Class, Student, Turn, Degree

log = logging.getLogger(__name__)


def escape(string):
    return str(re.sub(re.compile("[^\w\d\s.-]", re.UNICODE), "", string))


class Database:
    def __init__(self, file=os.path.dirname(__file__) + '/CLIP.db'):
        log.debug("Establishing a database connection to file:'{}'".format(file))
        self.link = sqlite3.connect(file, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self.cursor = self.link.cursor()
        self.lock = threading.Lock()

        # full caches
        self.institutions = {}  # [clip_id] -> Institution
        self.departments = {}  # [clip_id] -> Department
        self.degrees = {}  # [clip_id] -> Degree
        self.periods = {}  # [letter][stage] -> Period
        self.courses = {}  # [clip_id] -> Course
        self.course_abbreviations = {}  # [abbr] -> Course
        self.weekdays = {}  # [portuguese_name] -> internal_id
        self.turn_types = {}  # [abbr] -> db_id
        self.teachers = {}  # [name] -> db_id

        # partial caches (built as requests are made)
        self.class_id_cache = {}  # TODO check if this cannot be removed and the bellow cache used instead
        self.class_cache = {}

        self.__load_cached_collections__()

    def __load_cached_collections__(self):
        log.debug("Building cached collections")
        self.__load_institutions__()
        self.__load_degrees__()
        self.__load_periods__()
        self.__load_departments__()
        self.__load_courses__()
        self.__load_weekdays__()
        self.__load_turn_types__()
        self.__load_teachers__()
        log.debug("Finished building cache")

    def __load_institutions__(self):
        log.debug("Building institution cache")
        institutions = {}
        self.lock.acquire()
        try:
            self.cursor.execute('SELECT internal_id, id, abbreviation, name, initial_year, last_year '
                                'FROM Institutions')
            for institution in self.cursor:
                institutions[institution[0]] = Institution(
                    institution[0], institution[2], name=institution[3],
                    initial_year=institution[4], last_year=institution[5], db_id=institution[1])
        finally:
            self.lock.release()
        self.institutions = institutions

    def __load_degrees__(self):
        log.debug("Building degree cache")
        degrees = {}
        self.lock.acquire()
        try:
            self.cursor.execute('SELECT internal_id, id, name_en '
                                'FROM Degrees '
                                'WHERE internal_id NOT NULL')
            for degree in self.cursor:
                degrees[degree[0]] = Degree(degree[0], degree[2], db_id=degree[1])
        finally:
            self.lock.release()
        self.degrees = degrees

    def __load_periods__(self):
        log.debug("Building period cache")
        periods = {}
        self.lock.acquire()
        try:
            self.cursor.execute('SELECT type_letter, stage, stages, id '
                                'FROM Periods')
            for period in self.cursor:
                if period[0] not in periods:  # unseen letter
                    periods[period[0]] = {}

                periods[period[0]][period[1]] = Period(period[1], period[2], period[0], db_id=period[3])
        finally:
            self.lock.release()
        self.periods = periods

    def __load_departments__(self):
        log.debug("Building department cache")
        departments = {}
        self.lock.acquire()
        try:
            self.cursor.execute('SELECT Departments.internal_id, Departments.id, Departments.name, '
                                'Departments.initial_year, Departments.last_year, Institutions.internal_id '
                                'FROM Departments '
                                'JOIN Institutions ON Departments.institution = Institutions.id')
            for department in self.cursor:
                departments[department[0]] = Department(
                    department[0], department[2], self.institutions[department[5]],
                    initial_year=department[3], last_year=department[4], db_id=department[1])
        finally:
            self.lock.release()
        self.departments = departments

    def __load_courses__(self):
        log.debug("Building course cache")
        courses = {}
        course_abbreviations = {}

        self.lock.acquire()
        try:
            self.cursor.execute(
                'SELECT internal_id, name, abbreviation, id, initial_year, last_year, degree, institution '
                'FROM Courses '
                'WHERE abbreviation IS NOT NULL')
            for course in self.cursor:
                course_obj = Course(course[0], course[1], course[2], course[6], course[7],
                                    initial_year=course[4], last_year=course[5], db_id=course[3])
                courses[course[0]] = course_obj

                if course[2] not in course_abbreviations:
                    course_abbreviations[course[2]] = []
                course_abbreviations[course[2]].append(course_obj)
        finally:
            self.lock.release()
        self.courses = courses
        self.course_abbreviations = course_abbreviations

    def __load_weekdays__(self):
        log.debug("Building weekdays cache")
        weekdays = {}
        self.lock.acquire()
        try:
            self.cursor.execute('SELECT id, name_pt '
                                'FROM Weekdays')
            for weekday in self.cursor:
                weekdays[weekday[1]] = weekday[0]
        finally:
            self.lock.release()
        self.weekdays = weekdays

    def __load_turn_types__(self):
        log.debug("Building turn types cache")
        turn_types = {}
        self.lock.acquire()
        try:
            self.cursor.execute('SELECT abbr, id, type '
                                'FROM TurnTypes')
            for turn_type in self.cursor:
                turn_types[turn_type[0]] = {'id': turn_type[1], 'name': turn_type[2]}
        finally:
            self.lock.release()
        self.turn_types = turn_types

    def __load_teachers__(self):
        log.debug("Building teacher cache")
        teachers = {}
        self.lock.acquire()
        try:
            self.cursor.execute('SELECT id, name '
                                'FROM Teachers')
            for teacher in self.cursor:
                teachers[teacher[1]] = teacher[0]
        finally:
            self.lock.release()
        self.teachers = teachers

    def add_institutions(self, institutions):  # bulk addition to avoid pointless cache reloads
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

        if len(institutions) > 0:
            print("{} institutions added successfully!".format(len(institutions)))

        self.__load_institutions__()

    def add_departments(self, departments):  # bulk addition to avoid pointless cache reloads
        for department in departments:
            exists = False

            if department.identifier in self.departments:
                stored_department = self.departments[department.identifier]
                exists = True
                if stored_department.name != department.name:
                    raise Exception("Different departments had an id collision:\n\tStored: {}\n\tNew: {}".format(
                        stored_department, department))

                # creation date different or  last year different
                if department.initial_year != stored_department.initial_year \
                        or department.last_year != stored_department.last_year:
                    print("Updating department from:'{}' to:'{}'".format(stored_department, department))
                    self.lock.acquire()
                    try:
                        self.cursor.execute(
                            'UPDATE Departments '
                            'SET initial_year=?, last_year=? '
                            'WHERE internal_id=? AND institution=?',
                            (department.initial_year, department.last_year, department.identifier,
                             department.institution.db_id))
                    finally:
                        self.lock.release()

            if not exists:  # unknown department, add it to the DB
                print("Adding department {}".format(department))
                self.lock.acquire()
                try:
                    self.cursor.execute(
                        'INSERT INTO Departments(internal_id, name, initial_year, last_year, institution) '
                        'VALUES (?, ?, ?, ?, ?)',
                        (department.identifier, department.name, department.initial_year, department.last_year,
                         department.institution.db_id))
                finally:
                    self.lock.release()

        self.lock.acquire()
        try:
            self.link.commit()
        finally:
            self.lock.release()

        if len(departments) > 0:
            print("{} departments added successfully!".format(len(departments)))
        self.__load_departments__()

    def add_class(self, class_: Class, commit=False) -> Class:
        if class_.department not in self.departments:
            raise Exception("Unknown department")

        department = self.departments[class_.department]

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
                class_.db_id = stored_class[0]
                if commit:
                    self.lock.acquire()
                    try:
                        self.link.commit()
                    finally:
                        self.lock.release()

                return class_

        # class isn't stored yet
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

            class_.db_id = self.cursor.fetchone()[0]

            if commit:
                self.link.commit()

            return class_
        finally:
            self.lock.release()

    def add_class_instances(self, instances):
        # add class instance to the database
        for instance in instances:
            self.lock.acquire()
            try:
                self.cursor.execute('SELECT period, year '
                                    'FROM ClassInstances '
                                    'WHERE class=?',
                                    (instance.parent_class.db_id,))

                exists = False

                for stored_instance in self.cursor.fetchall():  # for every instance matching this instance parent class
                    if stored_instance[0] == instance.period and stored_instance[1] == instance.year:
                        exists = True
                        break

                if not exists:  # unknown class instance, add it to the DB
                    print("Adding instance of {}".format(instance))
                    self.cursor.execute('INSERT INTO ClassInstances(class, period, year) '
                                        'VALUES (?, ?, ?)',
                                        (instance.parent_class.db_id, instance.period.db_id, instance.year))
            finally:
                self.lock.release()

        self.lock.acquire()
        try:
            self.link.commit()
        finally:
            self.lock.release()

        if len(instances) > 0:
            print("{} class instances added successfully!".format(len(instances)))

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

                if stored_course_degree != course.degree.db_id:
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
                         course.abbreviation, course.degree.db_id, self.institutions[course.institution].identifier))
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
                    self.lock.acquire()
                    try:
                        self.cursor.execute(
                            'UPDATE Courses '
                            'SET degree=? '
                            'WHERE internal_id=? AND institution=?',
                            (course.degree.db_id, course.identifier, self.institutions[course.institution].identifier))
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
    def add_student(self, student: Student, commit=False) -> Student:

        course_db_id = None if student.course is None else student.course.db_id
        institution_id = None if student.institution is None else student.institution.db_id
        abbreviation = student.abbreviation if student.abbreviation != '' else None
        abbreviation = None if abbreviation is None else abbreviation.strip()
        abbreviation = None if abbreviation == '' else abbreviation

        if student.name is None or student.name == '':
            raise Exception("Invalid name")

        self.lock.acquire()
        try:
            if institution_id is None:
                self.cursor.execute('SELECT id, abbreviation, course, institution '
                                    'FROM Students '
                                    'WHERE internal_id=? AND name=?',
                                    (student.identifier, student.name))
            else:
                self.cursor.execute('SELECT id, abbreviation, course, institution '
                                    'FROM Students '
                                    'WHERE internal_id=? AND name=? AND institution=?',
                                    (student.identifier, student.name, institution_id))
            matching_students = self.cursor.fetchall()
        finally:
            self.lock.release()

        registered = False
        if len(matching_students) == 0:  # new student, add him
            self.lock.acquire()
            try:
                self.cursor.execute('INSERT INTO STUDENTS(name, internal_id, abbreviation, course, institution) '
                                    'VALUES (?, ?, ?, ?, ?)',
                                    (student.name, student.identifier, student.abbreviation,
                                     course_db_id, institution_id))
                print("New student saved: {})".format(student))  # TODO proper logging
            finally:
                self.lock.release()
        elif len(matching_students) == 1:
            registered = True
        else:  # bug or several institutions (don't know if it is even possible)
            raise Exception("Duplicated student found: {}".format(student))

        if registered:
            stored_id = matching_students[0][0]
            stored_abbr = matching_students[0][1]
            stored_course = matching_students[0][2]
            stored_institution = matching_students[0][3]

            if abbreviation is not None and stored_abbr is not None and stored_abbr != abbreviation:
                raise Exception("Abbreviation mismatch. {} != {}".format(abbreviation, stored_abbr))

            if stored_abbr != abbreviation or stored_course != course_db_id or stored_institution != institution_id:

                new_abbr = stored_abbr if abbreviation is None else abbreviation
                new_institution = stored_institution if institution_id is None else institution_id
                new_course = stored_course if student.course is None else course_db_id

                self.lock.acquire()
                try:
                    self.cursor.execute("UPDATE STUDENTS "
                                        "SET abbreviation=?, institution=?, course=? "
                                        "WHERE id=?",
                                        (new_abbr, new_institution, new_course, stored_id))
                    print("Updated student info: {}".format(student))  # TODO proper logging
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
                                (student.identifier, student.name))
            if commit:
                self.link.commit()
            student.db_id = self.cursor.fetchone()[0]
            return student
        finally:
            self.lock.release()

    def add_turn(self, turn: Turn, commit=False):
        self.lock.acquire()
        try:
            self.cursor.execute(
                'SELECT id, restrictions, hours, enrolled, capacity, routes, state '
                'FROM Turns '
                'WHERE class_instance=? AND number=? AND type=?')
            turns = self.cursor.fetchall()
        finally:
            self.lock.release()

        type_id = self.turn_types[turn.type]

        if len(turns) > 1:
            raise Exception("We've got a consistency problem... Turn {} \nMatches:{}".format(turn, str(turns)))

        if len(turns) == 0:
            self.lock.acquire()
            try:
                self.cursor.execute(
                    'INSERT INTO Turns'
                    '(class_instance, number, type, restrictions, hours, enrolled, routes, capacity, state) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (turn.class_instance.db_id, turn.number, type_id, turn.restrictions,
                     turn.hours, turn.enrolled, turn.routes, turn.capacity, turn.state))
            finally:
                self.lock.release()

        old_turn_info = turns[0]
        new_turn = Turn(turn.class_instance, turn.number, turn.type, old_turn_info[3], old_turn_info[4],
                        hours=old_turn_info[2], routes=old_turn_info[5], state=old_turn_info[6], db_id=old_turn_info[0])
        del old_turn_info

        different = False

        if turn.restrictions is not None and turn.restrictions is not '':
            new_turn.restrictions = turn.restrictions
            different = True

        if turn.hours is not None:
            new_turn.hours = turn.hours
            different = True

        if turn.enrolled is not None:
            new_turn.enrolled = turn.enrolled
            different = True

        if turn.capacity is not None:
            new_turn.capacity = turn.capacity
            different = True

        if turn.routes is not None:
            new_turn.routes = turn.routes
            different = True

        if turn.state is not None:
            new_turn.state = turn.state
            different = True

        if different:
            self.lock.acquire()
            try:
                self.cursor.execute(
                    'UPDATE Turns '
                    'SET restrictions=?, hours=?, enrolled=?, capacity=?, routes=?, state=? '
                    'WHERE id=?',
                    (new_turn.restrictions, new_turn.hours, new_turn.enrolled, new_turn.capacity,
                     new_turn.routes, new_turn.state, new_turn.db_id))
            finally:
                self.lock.release()

        for teacher in turn.teachers:
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
                    (new_turn.db_id, self.teachers[teacher]))
                if self.cursor.fetchone() is None:
                    self.cursor.execute(
                        'INSERT INTO TurnTeachers(turn, teacher) '
                        'VALUES (?, ?)',
                        (new_turn.db_id, self.teachers[teacher]))
            finally:
                self.lock.release()

        if commit:
            self.lock.acquire()
            try:
                self.link.commit()
            finally:
                self.lock.release()

        return new_turn

    # Reconstructs the instances of a turn , IT'S DESTRUCTIVE!
    def add_turn_instances(self, instances):
        if len(instances) == 0:  # yes, yes, very smart!
            return
        turn_db_id = instances[0].turn.db_id

        self.lock.acquire()
        try:
            self.cursor.execute(
                'DELETE FROM TurnInstances '
                'WHERE turn=?', (turn_db_id,))

            for instance in instances:
                # FIXME Classrooms and Buildings
                self.cursor.execute(
                    'INSERT INTO TurnInstances(turn, start, end, weekday, classroom) '
                    'VALUES (?, ?, ?, ?, ?)',
                    (turn_db_id, instance.start, instance.end, instance.weekday, None))

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
                    ((None if admission.student is None else admission.student.db_id), admission.name,
                     admission.course.db_id, admission.phase, admission.year,
                     admission.option, admission.state, admission.check_date))
            finally:
                self.lock.release()
        self.lock.acquire()
        try:
            self.link.commit()
        finally:
            self.lock.release()

        if len(admissions) > 0:
            print("{} admissions added successfully!".format(len(admissions)))

    def add_enrollments(self, enrollments):
        self.lock.acquire()
        try:
            for enrollment in enrollments:
                try:
                    self.cursor.execute(
                        'INSERT INTO  Enrollments'
                        '(student_id, class_instance, attempt, student_year, statutes, observation) '
                        'VALUES (?, ?, ?, ?, ?, ?)',
                        (enrollment.student_id, enrollment.class_instance, enrollment.attempt,
                         enrollment.student_year, enrollment.statutes, enrollment.observation))
                except sqlite3.Error:
                    log.warning("Enrollment skipped {}".format(enrollment))
        finally:
            self.lock.release()
        self.lock.acquire()
        try:
            self.link.commit()
        finally:
            self.lock.release()

    def fetch_class_instances(self, year_asc=True, queue=False):
        class_instances = []
        if queue:
            class_instances = Queue()

        self.lock.acquire()
        try:
            if year_asc:
                self.cursor.execute('SELECT class_instance_id, period_id, year, class_id, class_iid, class_name, '
                                    'department_iid, institution_iid '
                                    'FROM ClassInstancesComplete '
                                    'ORDER BY year ASC')
            else:
                self.cursor.execute('SELECT class_instance_id, period_id, year, class_id, class_iid, class_name, '
                                    'department_iid, institution_iid '
                                    'FROM ClassInstancesComplete '
                                    'ORDER BY year DESC')

            for instance in self.cursor.fetchall():
                if instance[4] not in self.class_cache:
                    self.class_cache[instance[4]] = Class(
                        instance[4], instance[5], self.departments[instance[6]], db_id=instance[3])

                class_ = self.class_cache[instance[4]]
                if queue:
                    class_instances.put(
                        ClassInstance(class_, self.periods[instance[0]], instance[2], db_id=instance[0]))
                else:
                    class_instances.append(
                        ClassInstance(class_, self.periods[instance[0]], instance[2], db_id=instance[0]))

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
