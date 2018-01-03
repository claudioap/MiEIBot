from datetime import datetime

from clip import Database
from clip.utils import abbreviation_to_course, get_month_periods


class Controller:
    def __init__(self):
        self.database = Database()
        self.session = None

    def find_student(self, name, course_filter=None):
        return self.database.find_student(name)

    def find_course(self, abbreviation):
        abbreviation_to_course(self.database, abbreviation, year=datetime.now().year)

    def get_current_periods(self):
        periods = get_month_periods(self.database, datetime.now().month)