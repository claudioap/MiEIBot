from datetime import datetime


class IdentifiedEntity:
    def __init__(self, identifier, db_id=None):
        self.identifier = identifier
        self.db_id = db_id

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            if self.db_id is None or other.db_id is None:  # if there is no DB id
                return self.identifier == other.identifier  # use internal identifiers
            else:  # otherwise always compare using DB ids
                return self.db_id == other.db_id
        return False


class TemporalEntity(IdentifiedEntity):
    def __init__(self, identifier, initial_year=None, last_year=None, db_id=None):
        super().__init__(identifier, db_id=db_id)
        self.initial_year = initial_year
        self.last_year = last_year

    def has_time_range(self):
        return not (self.initial_year is None or self.last_year is None)

    def add_year(self, year):
        if self.initial_year is None:
            self.initial_year = year
        if self.last_year is None:
            self.last_year = year

        if self.initial_year > year:
            self.initial_year = year
        elif self.last_year < year:
            self.last_year = year

    def __str__(self):
        return ('' if self.initial_year is None or self.last_year is None else ' {} - {}'.format(
            self.initial_year, self.last_year))


class Institution(TemporalEntity):
    def __init__(self, identifier, abbreviation, initial_year=None, last_year=None, name=None, db_id=None):
        super().__init__(identifier, initial_year=initial_year, last_year=last_year, db_id=db_id)
        self.abbreviation = abbreviation
        self.name = name if name is not None else abbreviation
        self.db_id = db_id

    def __str__(self):
        return ("{}(id:{} db:{})".format(
            (self.name if self.name is not None else self.abbreviation), self.identifier, self.db_id)
                + super().__str__())


class Department(TemporalEntity):
    def __init__(self, identifier, name: str, institution: Institution, initial_year=None, last_year=None, db_id=None):
        super().__init__(identifier, last_year=last_year, db_id=db_id)
        self.name = name
        self.institution = institution
        self.initial_year = initial_year
        self.last_year = last_year
        self.db_id = db_id

    def __str__(self):
        return ("{}(id:{}, inst:{}, db:{})".format(
            self.name, self.identifier, self.institution, self.db_id)
                + super().__str__())


class Period:
    def __init__(self, stage: int, stages: int, letter=None, db_id=None):
        self.stage = stage
        self.stages = stages
        self.letter = letter
        self.db_id = db_id

    def __str__(self):
        return ("{} out of {}({})".format(self.stage, self.stages, self.letter)
                + ('' if self.db_id is None else ' (DB:{})'.format(self.db_id)))


class Degree(IdentifiedEntity):
    def __init__(self, identifier, name, db_id=None):
        super().__init__(identifier, db_id)
        self.name = name

    def __str__(self):
        return ("{}({})".format(self.name, self.identifier)
                + '' if self.db_id is None else ' (DB:{})'.format(self.db_id))


class Class(IdentifiedEntity):
    def __init__(self, identifier, name, department, db_id=None):
        super().__init__(identifier, db_id=db_id)
        self.name = name
        self.department = department
        self.db_id = db_id

    def __str__(self):
        return ("{}(id:{}, dept:{}, db:{})".format(
            self.name, self.identifier, self.department, self.db_id))


class ClassInstance:
    def __init__(self, parent_class: Class, period: Period, year: int, db_id=None):
        self.parent_class = parent_class
        self.period = period
        self.year = year
        self.db_id = db_id

    def __str__(self):
        return "{} on period {} of {}".format(self.parent_class, self.period, self.year)


class Course(TemporalEntity):
    def __init__(self, identifier, name: str, abbreviation: str, degree: Degree, institution: Institution,
                 initial_year=None, last_year=None, db_id=None):
        super().__init__(identifier, initial_year, last_year, db_id=db_id)
        self.name = name
        self.abbreviation = abbreviation
        self.degree = degree
        self.institution = institution

    def __str__(self):
        return ("{}(id:{} abbr:{}, deg:{} inst:{}, db:{})".format(
            self.name, self.identifier, self.abbreviation, self.degree, self.institution, self.db_id)
                + super().__str__())


class Student(IdentifiedEntity):
    def __init__(self, identifier, name: str, abbreviation=None, course=None, institution=None, db_id=None):
        super().__init__(identifier, db_id=db_id)
        self.name = name
        self.abbreviation = abbreviation
        self.course = course
        self.institution = institution
        self.db_id = db_id

    def __str__(self):
        return ("{} ({}, {})".format(self.name, self.identifier, self.abbreviation)
                + ('' if self.db_id is None else ' (DB:{})'.format(self.db_id)))


class Admission:
    def __init__(self, student, name: str, course: Course, phase: int, year: int, option: int, state,
                 check_date=None, db_id=None):
        self.student = student
        self.name = name
        self.course = course
        self.phase = phase
        self.year = year
        self.option = option
        self.state = state
        self.check_date = check_date if check_date is not None else datetime.now()
        self.class_db_id = db_id

    def __str__(self):
        return ("{}, admitted to {}({}) (option {}) at the phase {} of the {} contest. {} as of {}".format(
            (self.student.name if self.student is not None else self.name),
            self.course.name, self.course.identifier, self.option, self.phase, self.year, self.state, self.check_date)
                + (' (DB: {})'.format(self.class_db_id) if self.class_db_id is not None else ''))


class Enrollment:
    def __init__(self, student: Student, class_instance: ClassInstance, attempt: int, student_year: int,
                 statutes, observation):
        self.student = student
        self.class_instance = class_instance
        self.attempt = attempt
        self.student_year = student_year
        self.statutes = statutes
        self.observation = observation

    def __str__(self):
        return "{} enrolled to {}, attempt:{}, student year:{}, statutes:{}, obs:{}".format(
            self.student, self.class_instance, self.attempt, self.student_year, self.statutes, self.observation)


class Building:
    def __init__(self, name: str, institution: Institution, db_id=None):
        self.name = name
        self.institution = institution
        self.db_id = db_id

    def __str__(self):
        return "{}, {} (DB: {})".format(self.name, self.institution.name, self.db_id)

    def __hash__(self):
        return hash(self.name) + hash(self.institution.identifier)

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            if self.db_id is None or other.db_id is None:  # if there is no DB id
                return self.name == other.name and self.institution == other.institution
            else:  # otherwise always compare using DB ids
                return self.db_id == other.db_id
        return False


class Classroom(IdentifiedEntity):
    def __init__(self, name, building: Building, db_id=None):
        super().__init__(name, db_id)
        self.building = building

    def __str__(self):
        return "{}, {} (DB: {})".format(self.identifier, self.building.name, self.db_id)


class TurnType:
    def __init__(self, type, abbreviation, db_id=None):
        self.type = type
        self.abbreviation = abbreviation
        self.db_id = db_id

    def __str__(self):
        return self.type


class Turn:
    def __init__(self, class_instance: ClassInstance, number: int, turn_type: TurnType, enrolled: int, capacity: int,
                 hours=None, routes=None, restrictions=None, state=None, teachers=list(), db_id=None):
        self.class_instance = class_instance
        self.number = number
        self.type = turn_type
        self.enrolled = enrolled
        self.capacity = capacity
        self.hours = hours
        self.routes = routes
        self.restrictions = restrictions
        self.state = state
        self.teachers = teachers
        self.db_id = db_id

    def __str__(self):
        return "turn {}.{} of {} {}/{} students, {} hours, {} routes, state={}, teachers={}".format(
            self.type, self.number, self.class_instance, self.enrolled, self.capacity,
            self.hours, self.routes, self.state, len(self.teachers))


class TurnInstance:
    def __init__(self, turn: Turn, start: int, end: int, weekday, classroom=None):
        self.turn = turn
        self.start = start
        self.end = end
        self.weekday = weekday
        self.classroom = classroom

    @staticmethod
    def time_str(time):
        return "{}:{}".format(time / 60, time % 60)

    def __str__(self):
        return "{} to {}, day:{}, turn{}".format(
            self.time_str(self.start), self.time_str(self.end), self.weekday, self.turn)
