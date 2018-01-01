import logging
from queue import Queue
from threading import Thread, Lock
from unicodedata import normalize

import re

from clip import urls, Database, Session
from clip.entities import ClassInstance, Class, Enrollment, Department, Student, Turn, TurnInstance, Admission, \
    Institution, Classroom, Building
from clip.utils import parse_clean_request, abbreviation_to_course, weekday_to_id

log = logging.getLogger(__name__)


class PageCrawler(Thread):
    def __init__(self, name, clip_session: Session, database: Database, work_queue: Queue, queue_lock: Lock,
                 crawl_function):
        Thread.__init__(self)
        self.name = name
        self.session = clip_session
        self.database_link = database
        self.work_queue = work_queue
        self.queue_lock = queue_lock
        self.crawl_function = crawl_function

    def run(self):
        while True:
            self.queue_lock.acquire()
            if not self.work_queue.empty():
                work_unit = self.work_queue.get()
                self.queue_lock.release()
                self.crawl_function(self.session, self.database_link, work_unit)
            else:
                self.queue_lock.release()
                break


def crawl_classes(session: Session, database: Database, department: Department):
    classes = {}
    class_instances = []

    period_exp = re.compile('&tipo_de_per%EDodo_lectivo=(?P<type>\w)&per%EDodo_lectivo=(?P<stage>\d)$')
    class_exp = re.compile('&unidade_curricular=(\d+)')

    # for each year this department operated
    for year in range(department.initial_year, department.last_year + 1):
        hierarchy = parse_clean_request(session.get(
            urls.CLASSES.format(department.institution.identifier, year, department.identifier)))

        period_links = hierarchy.find_all(href=period_exp)

        # for each period this department teaches
        for period_link in period_links:
            match = period_exp.search(period_link.attrs['href'])
            period_type = match.group("type")
            stage = int(match.group("stage"))

            if period_type not in database.periods:
                raise Exception("Unknown period")

            period = database.periods[period_type][stage]
            hierarchy = parse_clean_request(session.get(urls.CLASSES_PERIOD.format(
                period_type, department.identifier, year, stage, department.institution.identifier)))

            class_links = hierarchy.find_all(href=class_exp)

            # for each class in this period
            for class_link in class_links:
                class_id = class_exp.findall(class_link.attrs['href'])[0]
                class_name = class_link.contents[0].strip()
                if class_id not in classes:
                    classes[class_id] = database.add_class(Class(class_id, class_name, department.identifier))

                class_instances.append(ClassInstance(classes[class_id], period, year))

        database.commit()
    database.add_class_instances(class_instances)


def crawl_admissions(session: Session, database: Database, institution: Institution):
    admissions = []
    course_exp = re.compile("\\bcurso=(\d+)$")
    years = range(institution.initial_year, institution.last_year + 1)
    for year in years:
        courses = set()
        hierarchy = parse_clean_request(session.get(urls.ADMISSIONS.format(year, institution.identifier)))
        course_links = hierarchy.find_all(href=course_exp)
        for course_link in course_links:  # find every course that had students that year
            courses.add(course_exp.findall(course_link.attrs['href'])[0])

        for course_id in courses:
            course = database.courses[course_id]
            for phase in range(1, 4):  # for every of the three phases
                hierarchy = parse_clean_request(
                    session.get(urls.ADMITTED.format(year, institution.identifier, phase, course_id)))
                # find the table structure containing the data (only one with those attributes)
                table_root = hierarchy.find('th', colspan="8", bgcolor="#95AEA8").parent.parent

                for tag in table_root.find_all('th'):  # for every table header
                    if tag.parent is not None:
                        tag.parent.decompose()  # remove its parent row

                table_rows = table_root.find_all('tr')
                for table_row in table_rows:  # for every student admission
                    table_row = list(table_row.children)

                    # take useful information
                    name = table_row[1].text.strip()
                    option = table_row[9].text.strip()
                    student_iid = table_row[11].text.strip()
                    state = table_row[13].text.strip()

                    student_iid = student_iid if student_iid != '' else None
                    option = None if option == '' else int(option)
                    state = state if state != '' else None

                    student = None

                    if student_iid is not None:  # if the student has an id add him/her to the database
                        student = database.add_student(
                            Student(student_iid, name, course=course, institution=institution))

                    name = name if student is None else None
                    admission = Admission(student, name, course, phase, year, option, state)
                    log.debug("Found admission: {}".format(admission))
                    admissions.append(admission)
    database.add_admissions(admissions)


def crawl_class_instance(session: Session, database: Database, class_instance: ClassInstance):
    institution = class_instance.parent_class.department.institution

    hierarchy = parse_clean_request(
        session.get(urls.CLASS_ENROLLED.format(
            class_instance.period.letter, class_instance.parent_class.department.identifier,
            class_instance.year, class_instance.period.stage, institution.identifier,
            class_instance.parent_class.identifier)))

    # Strip file header and split it into lines
    content = hierarchy.text.splitlines()[4:]

    enrollments = []

    for line in content:  # for every student enrollment
        information = line.split('\t')
        if len(information) != 7:
            if len(hierarchy.find_all(string=re.compile("Pedido inválido"))) > 0:
                log.debug("Instance skipped")
                return
            else:
                log.warning("Invalid line")
                continue
        # take useful information
        student_statutes = information[0].strip()
        student_name = information[1].strip()
        student_iid = information[2].strip()
        student_abbr = information[3].strip()
        course_abbr = information[4].strip()
        attempt = int(information[5].strip().rstrip('ºª'))
        student_year = int(information[6].strip().rstrip('ºª'))

        course = abbreviation_to_course(database, course_abbr, year=class_instance.year)

        # TODO consider sub-courses EG: MIEA/[Something]
        observation = course_abbr if course is not None else (course_abbr + "(Unknown)")
        # update student info and take id
        student = database.add_student(
            Student(student_iid, student_name, abbreviation=student_abbr, course=course, institution=institution))

        enrollment = Enrollment(student, class_instance, attempt, student_year, student_statutes, observation)
        enrollments.append(enrollment)
        log.debug("Enrollment found: {}".format(enrollment))

    database.add_enrollments(enrollments)


def crawl_class_turns(session: Session, database: Database, class_instance: ClassInstance):
    institution = class_instance.parent_class.department.institution
    hierarchy = parse_clean_request(
        session.get(urls.TURNS_INFO.format(
            class_instance.parent_class.identifier, institution.identifier,
            class_instance.year, class_instance.period.letter,
            class_instance.period.stage, class_instance.parent_class.department.identifier)))

    turn_link_exp = re.compile("\\b&tipo=(?P<type>\\w)+&n%BA=(?P<number>\\d+)\\b")
    schedule_exp = re.compile(  # extract turn information
        '(?P<weekday>[\\w-]+) {2}'
        '(?P<init_hour>\\d{2}):(?P<init_min>\\d{2}) - (?P<end_hour>\\d{2}):(?P<end_min>\\d{2})(?: {2})?'
        '(?:Ed .*: (?P<room>[\\w\\b. ]+)/(?P<building>[\\w\\d. ]+))?')

    turn_links = hierarchy.find_all(href=turn_link_exp)

    # When there is only one turn, the received page is the turn itself.
    single_turn = False
    turn_count = 0  # consistency check

    turn_type = None
    turn_number = None

    for turn_link in turn_links:
        if "aux=ficheiro" in turn_link.attrs['href'].lower():
            # there are no file links in turn lists, but they do exist on turn pages
            single_turn = True
        else:
            turn_count += 1
            turn_link_expression = turn_link_exp.search(turn_link.attrs['href'])
            turn_type = turn_link_expression.group("type")
            turn_number = int(turn_link_expression.group("number"))

    turn_pages = []  # pages for turn parsing
    if single_turn:  # if the loaded page is the only turn
        if turn_count > 1:
            raise Exception("Too many turns for a single turn...")
        if turn_count == 0:
            log.warning("Turn page without any turn. Skipping")
            return
        turn_pages.append((hierarchy, turn_type, turn_number))  # save it, avoid requesting it again
    else:  # if there are multiple turns then request them
        for turn_link in turn_links:
            turn_page = parse_clean_request(session.get(urls.ROOT + turn_link.attrs['href']))
            turn_link_expression = turn_link_exp.search(turn_link.attrs['href'])
            turn_type = turn_link_expression.group("type")
            turn_number = int(turn_link_expression.group("number"))
            turn_pages.append((turn_page, turn_type, turn_number))  # and save them with their metadata

    del hierarchy

    for page in turn_pages:  # for every turn in this class instance
        instances = []
        routes = []
        teachers = []
        restrictions = None
        weekly_minutes = None
        state = None
        enrolled = None
        capacity = None
        students = []
        turn_type = None

        # it it is an unknown turn type
        if page[1] in database.turn_types:
            turn_type = database.turn_types[page[1]]
        else:
            log.error("Unknown turn type: " + page[1])

        # turn information table
        info_table_root = page[0].find('th', colspan="2", bgcolor="#aaaaaa").parent.parent

        for tag in info_table_root.find_all('th'):  # for every table header
            if tag.parent is not None:
                tag.parent.decompose()  # remove its parent row

        information_rows = info_table_root.find_all('tr')
        del info_table_root

        fields = {}
        previous_key = None
        for table_row in information_rows:
            if len(table_row.contents) < 3:  # several lines ['\n', 'value']
                fields[previous_key].append(normalize('NFKC', table_row.contents[1].text.strip()))
            else:  # inline info ['\n', 'key', '\n', 'value']
                key = table_row.contents[1].text.strip().lower()
                previous_key = key
                fields[key] = [normalize('NFKC', table_row.contents[3].text.strip())]

        del previous_key

        for field, content in fields.items():
            if field == "marcação":
                for row in content:
                    information = schedule_exp.search(row)
                    if information is None:
                        raise Exception("Bad schedule:" + str(information))

                    weekday = weekday_to_id(database, information.group('weekday'))
                    start = int(information.group('init_hour')) * 60 + int(information.group('init_min'))
                    end = int(information.group('end_hour')) * 60 + int(information.group('end_min'))

                    building = information.group('building')
                    room = information.group('room')

                    # TODO fix that classroom thingy, also figure a way to create the turn before its instances
                    if building is not None:
                        building = database.add_building(Building(building.strip(), institution))
                        if room is not None:  # if there is a room, use it
                            room = database.add_classroom(Classroom(room.strip(), building))
                        else:  # use building as room name
                            room = database.add_classroom(Classroom(building.name, building))
                    instances.append(TurnInstance(None, start, end, weekday, classroom=room))
            elif field == "turno":
                pass
            elif "percursos" in field:
                routes = content
            elif field == "docentes":
                for teacher in content:
                    teachers.append(teacher)
            elif "carga" in field:
                weekly_minutes = int(float(content[0].rstrip(" horas")) * 60)
            elif field == "estado":
                state = content[0]
            elif field == "capacidade":
                parts = content[0].split('/')
                try:
                    enrolled = int(parts[0])
                except ValueError:
                    log.warning("No enrolled information")
                try:
                    capacity = int(parts[1])
                except ValueError:
                    log.warning("No capacity information")
            elif field == "restrição":
                restrictions = content[0]
            else:
                raise Exception("Unknown field " + field)
        del fields

        student_table_root = page[0].find('th', colspan="4", bgcolor="#95AEA8").parent.parent

        for tag in student_table_root.find_all('th'):  # for every table header
            if tag.parent is not None:
                tag.parent.decompose()  # remove its parent row

        student_rows = student_table_root.find_all('tr')

        for student_row in student_rows:
            student_name = student_row.contents[1].text.strip()
            student_id = student_row.contents[3].text.strip()
            student_abbreviation = student_row.contents[5].text.strip()
            course_abbreviation = student_row.contents[7].text.strip()
            course = abbreviation_to_course(database, course_abbreviation, year=class_instance.year)

            # make sure he/she is in the db and have his/her db id
            student = database.add_student(Student(student_id, student_name, student_abbreviation, course=course))
            students.append(student)

        routes_str = None
        for route in routes:
            if routes_str is None:
                routes_str = route
            else:
                routes_str += (';' + route)

        turn = Turn(
            class_instance, page[2], turn_type, enrolled, capacity,
            minutes=weekly_minutes, routes=routes_str, restrictions=restrictions, state=state, teachers=teachers)
        turn = database.add_turn(turn)
        for instance in instances:
            instance.turn = turn

        database.add_turn_instances(instances)
        database.add_turn_students(turn, students)
