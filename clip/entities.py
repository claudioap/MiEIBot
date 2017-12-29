from datetime import datetime


class AbstractEntity:
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


class TemporalEntity(AbstractEntity):
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
    def __init__(self, identifier, name, institution, initial_year=None, last_year=None, db_id=None):
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


class Class(AbstractEntity):
    def __init__(self, identifier, name, department, db_id=None):
        super().__init__(identifier, db_id=db_id)
        self.name = name
        self.department = department
        self.db_id = db_id

    def __str__(self):
        return ("{}(id:{}, dept:{}, db:{})".format(
            self.name, self.identifier, self.department, self.db_id))


class ClassInstance:
    def __init__(self, class_id, period, year, class_db_id=None):
        self.class_id = class_id
        self.period = period
        self.year = year
        self.class_db_id = class_db_id

    def __str__(self):
        return "{} on period {} of {}".format(self.class_id, self.period, self.year)


class Course(TemporalEntity):
    def __init__(self, identifier, name, abbreviation, degree, institution,
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


class Admission:
    def __init__(self, student_id, name, course, phase, year, option, state, check_date=None, db_id=None):
        self.student_id = student_id
        self.name = name
        self.course = course
        self.phase = phase
        self.year = year
        self.option = option
        self.state = state
        self.check_date = check_date if check_date is not None else datetime.now()
        self.class_db_id = db_id

    def __str__(self):
        return ("{}, admitted to {} (option {}) at the phase {} of the {} contest. {} as of {}".format(
            (self.student_id if self.student_id is not None else self.name),
            self.course, self.option, self.phase, self.year, self.state, self.check_date)
                + (' (DB: {})'.format(self.class_db_id) if self.class_db_id is not None else ''))
