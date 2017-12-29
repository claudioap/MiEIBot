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
        self.identifier = identifier
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
        self.identifier = identifier
        self.name = name
        self.institution = institution
        self.initial_year = initial_year
        self.last_year = last_year
        self.db_id = db_id

    def __str__(self):
        return ("{}(id:{}, inst:{}, db:{})".format(
            self.name, self.identifier, self.institution, self.db_id)
                + super().__str__())
