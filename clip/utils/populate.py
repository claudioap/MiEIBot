import logging
import re
from datetime import datetime
from time import sleep
from threading import Lock

from clip import urls, Database
from clip.crawler import PageCrawler
from clip.utils import parse_clean_request, abbr_to_course_iid

logging.basicConfig(level=logging.INFO)
database = Database()
THREADS = 8


# TODO port dictionaries to classes

def populate_institutions(session):
    # Legacy code for fetching the years a given institution operated.
    # institution_years = []
    # soup = parse_clean_request(session.get(urls.INSTITUTION_YEARS.format(institution)))
    # year = re.compile("\\b\\d{4}\\b")
    # elements = soup.find_all(href=year)
    # for element in elements:
    #     match = int(year.findall(element.attrs['href'])[0])
    #     institution_years.append(match)
    pass  # TODO required for a fully automatic bootstrap


def populate_departments(session):
    departments = {}  # internal_id -> name
    for institution, institution_info in database.institutions.items():

        # Find the departments that existed under each year
        for year in range(institution_info['initial_year'], institution_info['last_year'] + 1):
            hierarchy = parse_clean_request(session.get(urls.DEPARTMENTS.format(year, institution)))
            department_exp = re.compile("\\bsector=\\d+\\b")
            department_links = hierarchy.find_all(href=department_exp)
            for department_link in department_links:
                match = department_exp.findall(department_link.attrs['href'])[0]
                department = str(match).split('=')[1]
                department_name = department_link.contents[0]

                if department in departments:  # update creation year
                    if departments[department]['institution'] != institution:
                        raise Exception("Department {}({}) found in different institutions ({} and {})".format(
                            department_name,
                            department,
                            institution,
                            departments[department]['institution']
                        ))
                    if departments[department]['creation'] > year:
                        departments[department]['creation'] = year
                    elif departments[department]['last_year'] < year:
                        departments[department]['last_year'] = year

                else:  # insert new
                    print("Found: {}({}) in {}".format(department_name, department, year))
                    departments[department] = {
                        'name': department_name,
                        'creation': year,
                        'last_year': year,
                        'institution': institution
                    }
    database.add_departments(departments)


def populate_classes(session):  # TODO threading

    for department, department_info in database.departments.items():  # for every department
        classes = {}
        class_instances = {}

        # for each year this department operated
        for year in range(department_info['initial_year'], department_info['final_year'] + 1):
            hierarchy = parse_clean_request(session.get(
                urls.CLASSES.format(department_info['institution'], year, department)))

            period_links = hierarchy.find_all(href=re.compile("\\bper%EDodo_lectivo=\\d\\b"))

            # for each period this department teaches
            for period_link in period_links:
                parts = period_link.attrs['href'].split('&')
                stage = parts[-1].split('=')[1]
                period_letter = parts[-2].split('=')[1]

                if period_letter not in database.periods:
                    raise Exception("Unknown period")  # TODO improve

                period_id = database.periods[period_letter][stage]
                hierarchy = parse_clean_request(session.get(
                    urls.CLASSES_PERIOD.format(
                        period_letter, department, year, stage, department_info['institution'])))

                class_links = hierarchy.find_all(href=re.compile("\\bunidade_curricular=\\b"))

                # for each class in this period
                for class_link in class_links:
                    class_iid = class_link.attrs['href'].split('&')[-1].split('=')[1]
                    class_name = class_link.contents[0]
                    print("Found {}({})".format(class_name, class_iid))
                    if class_iid not in classes:
                        classes[class_iid] = {
                            'id': None,
                            'name': class_name,
                            'department': department
                        }

                    if class_iid not in class_instances:
                        class_instances[class_iid] = []

                    class_instances[class_iid].append({
                        'period': period_id,
                        'year': year
                    })
        database.add_classes(classes)
        database.add_class_instances(class_instances)


def populate_courses(session):
    for institution in database.institutions:
        courses = {}
        hierarchy = parse_clean_request(session.get(urls.COURSES.format(institution)))
        course_exp = re.compile("\\bcurso=\\d+\\b")
        course_links = hierarchy.find_all(href=course_exp)
        for course_link in course_links:  # for every course link in the courses list page
            course_iid = course_exp.findall(course_link.attrs['href'])[0].split('=')[-1]
            courses[course_iid] = {
                'name': course_link.contents[0].text.strip(),
                'initial_year': None,
                'final_year': None,
                'abbreviation': None,
                'degree': None,
                'institution': institution
            }

            # fetch the course curricular plan to find the activity years
            hierarchy = parse_clean_request(session.get(urls.CURRICULAR_PLANS.format(institution, course_iid)))
            year_links = hierarchy.find_all(href=re.compile("\\bano_lectivo=\\d+\\b"))
            # find the extremes
            for year_link in year_links:
                year = int(year_link.attrs['href'].replace('\n', ' ').split('=')[-1])
                if courses[course_iid]['initial_year'] is None:
                    courses[course_iid]['initial_year'] = year
                    courses[course_iid]['final_year'] = year
                elif courses[course_iid]['initial_year'] > year:
                    courses[course_iid]['initial_year'] = year
                elif courses[course_iid]['final_year'] < year:
                    courses[course_iid]['final_year'] = year

        # fetch course abbreviation from the statistics page
        for degree in database.degrees:
            hierarchy = parse_clean_request(session.get(urls.STATISTICS.format(institution, degree)))
            course_links = hierarchy.find_all(href=course_exp)
            for course_link in course_links:
                course_iid = course_link.attrs['href'].split('=')[-1]
                abbr = course_link.contents[0]
                if course_iid in courses:
                    courses[course_iid]['abbreviation'] = abbr
                    courses[course_iid]['degree'] = database.degrees[degree]['id']
                else:
                    raise Exception("Course {}({}) was listed in the abbreviation list but wasn't found".format(
                        abbr, course_iid))

        database.add_courses(courses)


# populate student list from the national access contest (also obtain their preferences and current status)
def populate_nac_students(session):
    admissions = []
    for institution in database.institutions:
        for year in range(
                database.institutions[institution]['initial_year'],
                database.institutions[institution]['last_year'] + 1):
            courses = set()
            hierarchy = parse_clean_request(session.get(urls.ADMISSIONS.format(year, institution)))
            course_links = hierarchy.find_all(href=re.compile("\\bcurso=\\d+$"))
            for course_link in course_links:  # find every course that had students that year
                courses.add(course_link.attrs['href'].split('=')[-1])

            for course in courses:  # for every course
                if course not in database.courses:
                    raise Exception("Unknown course")

                for phase in range(1, 4):  # for every of the three phases
                    hierarchy = parse_clean_request(session.get(urls.ADMITTED.format(year, institution, phase, course)))
                    # find the table structure containing the data (only one with those attributes)
                    table_root = hierarchy.find('th', colspan="8", bgcolor="#95AEA8").parent.parent

                    for tag in hierarchy.find_all('th'):  # for every table header
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
                        document_number = document_number if document_number != '' else None
                        option = None if option == '' else int(option)
                        state = state if state != '' else None

                        student_id = None

                        if student_iid is not None:  # if the student has an iid add it to the database
                            student_id = database.add_student(student_iid, name, course=course, institution=institution)

                        print("Found {}(iid: {}). Admitted in the phase {} of {} (option:{}). Current state: {}".format(
                            name, student_iid, phase, year, option, state))
                        name = name if student_id is None else None
                        admissions.append({
                            'student_id': student_id,
                            'course': course,
                            'phase': phase,
                            'year': year,
                            'option': option,
                            'state': state,
                            'check_date': datetime.now(),
                            'name': name
                        })
    database.add_admissions(admissions)


def populate_class_instances(session):
    class_instances_queue = database.fetch_class_instances()
    class_instances_lock = Lock()

    threads = []
    for thread in range(0, THREADS):
        threads.append(
            PageCrawler(
                "Thread-" + str(thread),
                session,
                database,
                class_instances_queue,
                class_instances_lock,
                crawl_class_instance
            ))
        threads[thread].start()

    while True:
        class_instances_lock.acquire()
        if class_instances_queue.empty():
            class_instances_lock.release()
            break
        else:
            print(
                "*DING DING DONG* Your queue has approximately {} class instances remaining".format(
                    class_instances_queue.qsize()))
            class_instances_lock.release()
            sleep(5)

    for thread in threads:
        threads[thread].join()


def populate_database_from_scratch(session):
    populate_departments(session)
    populate_classes(session)
    populate_courses(session)
    populate_nac_students(session)
    populate_class_instances(session)


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
        course = abbr_to_course_iid(course_abbr, year=class_instance_info['year'])  # find course id
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
    database.add_enrollements(enrollments)
