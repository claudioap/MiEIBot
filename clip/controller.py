from clip import Database


class Controller:
    def __init__(self):
        self.database = Database()
        self.session = None

    def whois(self, name):
        return self.database.find_student(name)
