from threading import Thread
from unicodedata import normalize

import re

from clip import urls
from clip.utils import parse_clean_request, abbr_to_course_iid, weekday_to_id


class PageCrawler(Thread):
    def __init__(self, name, clip_session, database, work_queue, queue_lock, crawl_function):
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


def crawl_class_instance(session, database, class_instance_info):
    hierarchy = parse_clean_request(
        session.get(urls.CLASS_ENROLLED.format(
            class_instance_info['period_type'], class_instance_info['department'],
            class_instance_info['year'], class_instance_info['period'],
            class_instance_info['institution'], class_instance_info['class']
        )))

    # Strip file header and split it into lines
    content = hierarchy.text.splitlines()[4:]

    enrollments = []

    for line in content:  # for every student enrollment
        information = line.split('\t')
        if len(information) != 7:
            print("Invalid line")
            continue
        # take useful information
        student_statutes = information[0].strip()
        student_name = information[1].strip()
        student_iid = information[2].strip()
        student_abbr = information[3].strip()
        course_abbr = information[4].strip()
        attempt = int(information[5].strip().rstrip('ºª'))
        student_year = int(information[6].strip().rstrip('ºª'))

        print("{}({}, {}) being enrolled to {} for the {} time (grade {})".format(
            student_name, student_abbr, student_iid, class_instance_info['class'], attempt, student_year))
        course = abbr_to_course_iid(database, course_abbr, year=class_instance_info['year'])  # find course id
        # TODO consider sub-courses EG: MIEA/[Something]
        observation = course_abbr if course is not None else (course_abbr + "(Unknown)")
        # update student info and take id
        student_id = database.add_student_info(
            student_iid, student_name,
            course_id=course, abbr=student_abbr,
            institution_id=class_instance_info['institution'])
        enrollments.append({
            'student_id': student_id,
            'class_instance': class_instance_info['class_instance'],
            'attempt': attempt,
            'student_year': student_year,
            'statutes': student_statutes,
            'observation': observation
        })
    database.add_enrollments(enrollments)


def crawl_class_turns(session, database, class_instance_info):
    hierarchy = parse_clean_request(
        session.get(urls.TURNS_INFO.format(
            class_instance_info['class'], class_instance_info['institution'],
            class_instance_info['year'], class_instance_info['period_type'],
            class_instance_info['period'], class_instance_info['department'],
        )))

    turn_link_exp = re.compile("\\b&tipo=\\w+&n%BA=\\d+\\b")
    schedule_exp = re.compile(  # extract turn information
        '(?P<weekday>[\\w-]+) {2}'
        '(?P<init_hour>\\d{2}):(?P<init_min>\\d{2}) - (?P<end_hour>\\d{2}):(?P<end_min>\\d{2}) {2}'
        '(?:Ed (?P<building>[\\w\\b]{1,2}): (?P<room>(?:\\w* )?[\\w\\b.]{1,15})/.*'
        '|Ed: [\\w\\d.]+/(?P<alt_building>[\\w\\d. ]+))',
        re.UNICODE)

    turn_links = hierarchy.find_all(href=turn_link_exp)

    for turn_link in turn_links:  # for every turn in this class instance
        instances = []
        routes = []
        teachers = []
        restrictions = None
        weekly_hours = None
        state = None
        enrolled = None
        capacity = None
        students = []

        turn_link_str_parts = turn_link.attrs['href'].split('&')
        turn_type = turn_link_str_parts[-2].split('=')[1]
        turn_number = int(turn_link_str_parts[-1].split('=')[1])
        del turn_link_str_parts

        hierarchy = parse_clean_request(session.get(urls.ROOT + turn_link.attrs['href']))  # fetch turn

        # turn information table
        info_table_root = hierarchy.find('th', colspan="2", bgcolor="#aaaaaa").parent.parent

        for tag in info_table_root.find_all('th'):  # for every table header
            if tag.parent is not None:
                tag.parent.decompose()  # remove its parent row

        information_rows = info_table_root.find_all('tr')
        del info_table_root

        fields = {}
        previous_key = None
        for table_row in information_rows:
            if len(table_row.contents) < 3:  # several lines ['\n', 'value']
                fields[previous_key].append(normalize("NFKC", table_row.contents[1].text.strip()))
            else:  # inline info ['\n', 'key', '\n', 'value']
                key = table_row.contents[1].text.strip().lower()
                previous_key = key
                fields[key] = [normalize("NFKC", table_row.contents[3].text.strip())]

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
                    building = building if building is not None else information.group('alt_building')

                    instances.append({
                        'weekday': weekday,
                        'start': start,
                        'end': end,
                        'room': room,
                        'building': building
                    })
            elif field == "turno":
                pass
            elif "percursos" in field:
                routes = content
            elif field == "docentes":
                for teacher in content:
                    teachers.append(teacher)
            elif "carga" in field:
                weekly_hours = int(content[0].rstrip(" horas"))
            elif field == "estado":
                state = content[0]
            elif field == "capacidade":
                parts = content[0].split('/')
                enrolled = int(parts[0])
                capacity = int(parts[1])
            elif field == "restrição":
                restrictions = content[0]
            else:
                raise Exception("Unknown field " + field)
        del fields

        student_table_root = hierarchy.find('th', colspan="4", bgcolor="#95AEA8").parent.parent

        for tag in student_table_root.find_all('th'):  # for every table header
            if tag.parent is not None:
                tag.parent.decompose()  # remove its parent row

        student_rows = student_table_root.find_all('tr')

        for student_row in student_rows:
            student_name = student_row.contents[0].text.strip()
            student_iid = student_row.contents[3].text.strip()
            student_abbr = student_row.contents[5].text.strip()
            course_abbr = student_row.contents[7].text.strip()
            course_iid = abbr_to_course_iid(database, course_abbr)

            # make sure he/she is in the db and have his/her db id
            student_id = database.add_student_info(
                student_iid, student_name, course=course_iid, abbr=student_abbr, institution=None, commit=False)
            students.append(student_id)

        turn_id = database.add_turn(
            class_instance_info['class_instance'], turn_number, turn_type,
            restrictions=restrictions, weekly_hours=weekly_hours, enrolled=enrolled,
            teachers=teachers, routes=routes, state=state, capacity=capacity)

        database.add_turn_instances(turn_id, instances)
        database.add_turn_students(turn_id, students)
