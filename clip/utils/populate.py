import logging
import re
from queue import Queue
from time import sleep
from threading import Lock

from clip import urls
from clip.crawler import PageCrawler, crawl_class_turns, crawl_class_instance, crawl_classes
from clip.entities import Institution, Department, Admission, Course
from clip.utils import parse_clean_request

logging.basicConfig(level=logging.INFO)
THREADS = 8


def populate_institutions(session, database):
    institutions = []
    hierarchy = parse_clean_request(session.get(urls.INSTITUTIONS))
    link_exp = re.compile('/?institui%E7%E3o=(\d+)$')
    for institution_link in hierarchy.find_all(href=link_exp):
        clip_id = link_exp.findall(institution_link.attrs['href'])[0]
        abbreviation = institution_link.text
        institution = Institution(clip_id, abbreviation)
        institutions.append(institution)

    for institution in institutions:
        hierarchy = parse_clean_request(session.get(urls.INSTITUTION_YEARS.format(institution.identifier)))
        year_exp = re.compile("\b\d{4}\b")
        institution_links = hierarchy.find_all(href=year_exp)
        for institution_link in institution_links:
            year = int(year_exp.findall(institution_link.attrs['href'])[0])
            institution.add_year(year)

    for institution in institutions:
        print("Institution found: " + str(institution))

    database.add_institutions(institutions)


def populate_departments(session, database):
    departments = {}  # internal_id -> Department
    department_exp = re.compile('\bsector=(\d+)\b')
    for institution in database.institutions.values():

        if not institution.has_time_range():  # if it has no time range to iterate through
            continue

        # Find the departments that existed under each year
        for year in range(institution.initial_year, institution.last_year + 1):
            print("Crawling departments of institution {}. Year:{}".format(institution, year))
            hierarchy = parse_clean_request(session.get(urls.DEPARTMENTS.format(year, institution.identifier)))
            department_links = hierarchy.find_all(href=department_exp)
            for department_link in department_links:
                department_id = department_exp.findall(department_link.attrs['href'])[0]
                department_name = department_link.contents[0]

                if department_id in departments:  # update creation year
                    department = departments[department_id]
                    if department.institution != institution.identifier:
                        raise Exception("Department {}({}) found in different institutions ({} and {})".format(
                            department.name,
                            department_id,
                            institution.identifier,
                            department.institution
                        ))
                    department.add_year(year)
                else:  # insert new
                    department = Department(department_id, department_name, institution.identifier, year, year)
                    departments[department_id] = department
    print("Departments crawled. Database, its up to you!")
    database.add_departments(departments.values())


def populate_classes(session, database):
    department_queue = Queue()
    [department_queue.put(department) for department in database.departments.values()]
    department_lock = Lock()

    threads = []
    for thread in range(0, THREADS):
        threads.append(
            PageCrawler(
                "Thread-" + str(thread),
                session,
                database,
                department_queue,
                department_lock,
                crawl_classes
            ))
        threads[thread].start()

    while True:
        department_lock.acquire()
        if department_queue.empty():
            department_lock.release()
            break
        else:
            print(
                "{} departments remaining!".format(
                    department_queue.qsize()))
            department_lock.release()
            sleep(5)

    for thread in threads:
        thread.join()


def populate_courses(session, database):
    course_exp = re.compile("\\bcurso=(\d+)\\b")
    year_ext = re.compile("\\bano_lectivo=(\d+)\\b")
    for institution in database.institutions:
        courses = {}
        hierarchy = parse_clean_request(session.get(urls.COURSES.format(institution)))
        course_links = hierarchy.find_all(href=course_exp)
        for course_link in course_links:  # for every course link in the courses list page
            course_id = course_exp.findall(course_link.attrs['href'])[0]
            courses[course_id] = Course(
                course_id, course_link.contents[0].text.strip(),
                None, None, database.institutions[institution].identifier)

            # fetch the course curricular plan to find the activity years
            hierarchy = parse_clean_request(session.get(urls.CURRICULAR_PLANS.format(institution, course_id)))
            year_links = hierarchy.find_all(href=year_ext)
            # find the extremes
            for year_link in year_links:
                year = int(year_ext.findall(year_link.attrs['href'])[0])
                courses[course_id].add_year(year)

        # fetch course abbreviation from the statistics page
        for degree in database.degrees:
            hierarchy = parse_clean_request(session.get(urls.STATISTICS.format(institution, degree)))
            course_links = hierarchy.find_all(href=course_exp)
            for course_link in course_links:
                course_id = course_exp.findall(course_link.attrs['href'])[0]
                abbr = course_link.contents[0].strip()
                if course_id in courses:
                    courses[course_id].abbreviation = abbr
                    courses[course_id].degree = database.degrees[degree]['id']
                else:
                    raise Exception(
                        "{}({}) was listed in the abbreviation list but a corresponding course wasn't found".format(
                            abbr, course_id))

        database.add_courses(courses.values())


# populate student list from the national access contest (also obtain their preferences and current status)
def populate_nac_students(session, database):  # TODO threading (rework the database to save states apart), murder CLIP!
    admissions = []
    course_exp = re.compile("\\bcurso=(\d+)$")
    for institution in database.institutions:
        if not database.institutions[institution].has_time_range():  # if it has no time range to iterate through
            continue
        years = range(database.institutions[institution].initial_year, database.institutions[institution].last_year + 1)
        for year in years:
            courses = set()
            hierarchy = parse_clean_request(session.get(urls.ADMISSIONS.format(year, institution)))
            course_links = hierarchy.find_all(href=course_exp)
            for course_link in course_links:  # find every course that had students that year
                courses.add(course_exp.findall(course_link.attrs['href'])[0])

            for course in courses:  # for every course
                if course not in database.courses:
                    raise Exception("Unknown course")

                for phase in range(1, 4):  # for every of the three phases
                    hierarchy = parse_clean_request(session.get(urls.ADMITTED.format(year, institution, phase, course)))
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

                        student_id = None

                        if student_iid is not None:  # if the student has an iid add it to the database
                            student_id = database.add_student(student_iid, name, course=course, institution=institution)

                        name = name if student_id is None else None
                        admission = Admission(student_id, name, course, phase, year, option, state)
                        print("Found admission: {}".format(admission))
                        admissions.append(admission)
        database.add_admissions(admissions)


def populate_class_instances(session, database):
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


def populate_class_instances_turns(session, database):
    class_instances_queue = database.fetch_class_instances(year_asc=False)
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
                crawl_class_turns
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


def populate_database_from_scratch(session, database):
    populate_institutions(session, database)
    populate_departments(session, database)
    populate_classes(session, database)
    populate_courses(session, database)
    populate_nac_students(session, database)
    populate_class_instances(session, database)
