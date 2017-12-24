import logging
import sqlite3
import re
import os.path
from bs4 import BeautifulSoup
from clip import Session

logging.basicConfig(level=logging.INFO)

if not os.path.isfile(os.path.dirname(__file__) + '/clip.db'):
    print("No database")
    exit(1)
database = sqlite3.connect(os.path.dirname(__file__) + '/clip.db')

db_cursor = database.cursor()
db_cursor.execute('SELECT internal_id, id, short_name FROM Institutions')
institutions = {}
for row in db_cursor:
    institutions[row[0]] = {'id': row[1], 'name': row[2]}

degrees = {}
db_cursor.execute('SELECT id, internal_id, name_en FROM Degrees WHERE internal_id NOT NULL')
for row in db_cursor:
    degrees[row[1]] = {'id': row[0], 'name': row[2]}

url_institution_years = 'https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo?institui%E7%E3o={}'
url_departments = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo?ano_lectivo={}&institui%E7%E3o={}"
url_classes = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo/sector?institui%E7%E3o={}&ano_lectivo={}&sector={}"
url_classes_period = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo/sector/ano_lectivo?tipo_de_per%EDodo_lectivo={}&sector={}&ano_lectivo={}&per%EDodo_lectivo={}&institui%E7%E3o={}"
url_courses = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/curso?institui%E7%E3o={}"
url_curricular_plans = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/curso?institui%E7%E3o={}&curso={}"
url_admissions = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo/candidaturas?ano_lectivo={}&institui%E7%E3o={}"
url_admitted = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo/candidaturas/colocados?ano_lectivo={}&institui%E7%E3o={}&fase={}&curso={}"
url_class_enrolled = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo/sector/ano_lectivo/unidade_curricular/actividade/inscri%E7%F5es/pautas?tipo_de_per%EDodo_lectivo={}&sector={}&ano_lectivo={}&per%EDodo_lectivo={}&institui%E7%E3o={}&unidade_curricular={}&modo=pauta&aux=ficheiro"
url_class_exam_enrolled_file = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo/sector/ano_lectivo/unidade_curricular/actividade/testes_de_avalia%E7%E3o/inscritos?institui%E7%E3o={}&%EDndice={}&sector={}&ano_lectivo={}&tipo_de_per%EDodo_lectivo={}&tipo={}&per%EDodo_lectivo={}&unidade_curricular={}&%E9poca={}&aux=ficheiro"
url_exam_grades = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo/sector/ano_lectivo/unidade_curricular/actividade/testes_de_avalia%E7%E3o/inscritos?tipo_de_per%EDodo_lectivo={}&sector={}&ano_lectivo={}&per%EDodo_lectivo={}&institui%E7%E3o={}&unidade_curricular={}&%E9poca={}&tipo={}&%EDndice={}"
url_statistics = "https://clip.unl.pt/utente/institui%E7%E3o_sede/unidade_organica/ensino/estat%EDstica/alunos/evolu%E7%E3o?institui%E7%E3o={}&n%EDvel_acad%E9mico={}"
session = Session()


# Check institution years of existence
def institution_years(session, institution):
    institution_years = []
    soup = request_to_soup(session.get(url_institution_years.format(institution)))
    year = re.compile("\\b\\d{4}\\b")
    elements = soup.find_all(href=year)
    for element in elements:
        match = year.findall(element.attrs['href'])[0]
        institution_years.append(match)
    return institution_years


def populate_departments(session, database):
    db_cursor = database.cursor()
    for institution in institutions:
        departments = {}  # internal_id -> name
        # Fetch institution ID from DB
        db_cursor.execute('SELECT id FROM Institutions WHERE internal_id=?', (int(institution),))
        institution_row = db_cursor.fetchone()
        if institution_row is None or len(institution_row) == 0:
            raise Exception("Attempted to assign a department to an unknown institution")
        institution_id = institution_row[0]

        # Find institution years of activity
        years = institution_years(session, institution)
        for year in years:  # Find the departments that existed under each year
            soup = request_to_soup(session.get(url_departments.format(year, institution)))
            department_exp = re.compile("\\bsector=\\d+\\b")
            department_links = soup.find_all(href=department_exp)
            for department_link in department_links:
                match = department_exp.findall(department_link.attrs['href'])[0]
                department_iid = str(match).split('=')[1]
                department_name = department_link.contents[0]

                if department_iid in departments:  # update creation year
                    if (departments[department_iid]['institution'] != institution_id):
                        raise Exception("Department {}({}) found in different institutions id's {} and {}".format(
                            department_name,
                            department_iid,
                            institution_id,
                            departments[department_iid]['institution']
                        ))
                    if departments[department_iid]['creation'] > int(year):
                        departments[department_iid]['creation'] = int(year)
                    elif departments[department_iid]['last_year'] < int(year):
                        departments[department_iid]['last_year'] = int(year)

                else:  # insert new
                    print("Found: {}({}) in {}".format(department_name, department_iid, year))
                    departments[department_iid] = {
                        'name': department_name,
                        'creation': int(year),
                        'last_year': int(year),
                        'institution': institution_id
                    }

        # add departments to the database
        for department_iid, department_info in departments.items():
            db_cursor.execute('SELECT name, creation, last_year '
                              'FROM Departments '
                              'WHERE institution=? AND internal_id=?',
                              (institution_id, department_iid))
            exists = False
            different = False
            for row in db_cursor.fetchall():
                if row[0] != department_info['name']:
                    raise Exception("Different departments had an id collision")

                # creation date different or  last year different
                elif row[1] != int(department_info['creation']) or row[2] != int(department_info['last_year']):
                    different = True
                    exists = True

            if not exists:  # unknown department, add it to the DB
                print("Adding  {}({}) {} - {} to the database".format(
                    department_info['name'],
                    department_iid,
                    department_info['creation'],
                    department_info['last_year']))
                db_cursor.execute(
                    'INSERT INTO Departments(internal_id, name, creation, last_year, institution) '
                    'VALUES (?, ?, ?, ?, ?)',
                    (department_iid,
                     department_info['name'],
                     department_info['creation'],
                     department_info['last_year'],
                     institution_id))

            if different:  # department changed
                print("Updating department {} (now goes from {} to {})".format(
                    department_info['name'], department_info['creation'], department_info['last_year']))
                db_cursor.execute(
                    'UPDATE Departments '
                    'SET creation = ?, last_year=? '
                    'WHERE internal_id=? AND institution=?',
                    (department_info['creation'],
                     department_info['last_year'],
                     department_iid,
                     department_info['institution']))

        database.commit()
        print("Changes saved")


def populate_classes(session, database):
    db_cursor = database.cursor()
    db_cursor.execute('SELECT Departments.id, Departments.internal_id, Institutions.internal_id, '
                      'Departments.creation, Departments.last_year '
                      'FROM Departments JOIN Institutions '
                      'WHERE Departments.institution = Institutions.id')
    class_id_cache = {}
    class_department_cache = {}

    for row in db_cursor.fetchall():  # for every department
        classes = {}
        class_instances = {}
        department_id = row[0]
        department_iid = row[1]
        institution_iid = row[2]
        department_creation = row[3]
        department_last_year = row[4]
        period_ids = {}  # cache with [stages][stage] -> id

        # for each year this department operated
        for year in range(department_creation, department_last_year + 1):
            soup = request_to_soup(session.get(url_classes.format(institution_iid, year, department_iid)))
            period_links = soup.find_all(href=re.compile("\\bper%EDodo_lectivo=\\d\\b"))

            # for each period this department teaches
            for period_link in period_links:
                parts = period_link.attrs['href'].split('&')
                stage = parts[-1].split('=')[1]
                stages_letter = parts[-2].split('=')[1]
                if stages_letter == 't':
                    stages = 4
                elif stages_letter == 's':
                    stages = 2
                elif stages_letter == 'a':
                    stages = 1
                else:
                    raise Exception("Unknown period")

                if stages in period_ids and stage in period_ids[stages]:
                    period_id = period_ids[stages][stage]
                else:
                    db_cursor.execute('SELECT id FROM Periods WHERE stage=? AND stages=?',
                                      (stage, stages))
                    row = db_cursor.fetchone()
                    if row is None:
                        raise Exception("Unknown period")
                    else:
                        period_id = row[0]
                        if stages not in period_ids:
                            period_ids[stages] = {}
                        period_ids[stages][stage] = period_id

                soup = request_to_soup(
                    session.get(url_classes_period.format(stages_letter, department_iid, year, stage, institution_iid)))

                class_links = soup.find_all(href=re.compile("\\bunidade_curricular=\\b"))
                for class_link in class_links:
                    class_iid = class_link.attrs['href'].split('&')[-1].split('=')[1]
                    class_name = class_link.contents[0]
                    print("Found {}({})".format(class_name, class_iid))
                    if class_iid not in classes:
                        classes[class_iid] = {
                            'id': None,
                            'name': class_name,
                            'department': department_id
                        }
                        class_department_cache[class_iid] = department_id
                    if class_iid not in class_instances:
                        class_instances[class_iid] = []

                    class_instances[class_iid].append({
                        'period': period_id,
                        'year': year
                    })

        # add class to the database
        for class_iid, class_info in classes.items():
            db_cursor.execute('SELECT name '
                              'FROM Classes '
                              'WHERE internal_id=? AND department=?',
                              (class_iid, int(class_info['department'])))

            exists = False
            for row in db_cursor.fetchall():  # for every class matching the iid and department
                if row[0] != class_info['name']:  # if their names don't match
                    raise Exception("Different classes had an id collision {} with {}, id was {}".format(
                        class_info['name'], row[0], class_iid
                    ))
                else:  # identical class was already in the db
                    print("Class already known: {}({})".format(class_info['name'], class_iid))
                    exists = True

            if not exists:  # unknown class, add it to the DB
                print("Adding  {}({}) to the database".format(class_info['name'], class_iid))
                db_cursor.execute('INSERT INTO Classes(internal_id, name, department) VALUES (?, ?, ?)',
                                  (class_iid, class_info['name'], class_info['department']))

        database.commit()
        print("Changes saved")

        # fetch assigned class ids
        db_cursor.execute('SELECT id, internal_id, department FROM Classes')

        for row in db_cursor.fetchall():
            class_id = row[0]
            class_iid = row[1]
            class_department = row[2]

            if class_iid not in classes:  # class was not fetched now
                continue

            class_dict = classes[class_iid]
            if class_dict['department'] != class_department:  # if different department, this this is a different class
                continue

            class_dict['id'] = class_id  # class id

        # add class instance to the database
        for class_iid, class_instances_info in class_instances.items():
            # figure out if the class id is unknown
            if class_iid not in class_id_cache:
                # fetch it if needed
                db_cursor.execute('SELECT id '
                                  'FROM Classes '
                                  'WHERE internal_id=? AND department=?',
                                  (class_iid, class_department_cache[class_iid]))
                result = db_cursor.fetchone()
                if result is None:
                    raise Exception("Unknown class iid for {}, info: {}".format(class_iid, class_instances))
                class_id_cache[class_iid] = result[0]

            # obtain the class id from the cache
            class_id = class_id_cache[class_iid]

            for instance in class_instances_info:  # for every instance of a class
                db_cursor.execute('SELECT period, year '
                                  'FROM ClassInstances '
                                  'WHERE class=?',
                                  (class_id,))
                exists = False  # figure if it is already in the db
                for row in db_cursor.fetchall():  # for every class matching the iid and department
                    if row[0] == instance['period'] and row[1] == instance['year']:
                        exists = True
                        break

                if not exists:  # unknown class instance, add it to the DB
                    print("Adding  {}({}) instance in period {} of {} to the database".format(
                        classes[class_iid]['name'], classes[class_iid]['id'],
                        instance['period'], instance['year']))
                    db_cursor.execute('INSERT INTO ClassInstances(class, period, year) '
                                      'VALUES (?, ?, ?)',
                                      (class_id, instance['period'], instance['year']))

        database.commit()
        print("Changes saved")


def populate_courses(session, database):
    db_cursor = database.cursor()
    for institution in institutions:
        courses = {}
        soup = request_to_soup(session.get(url_courses.format(institution)))
        course_exp = re.compile("\\bcurso=\\d+\\b")
        course_links = soup.find_all(href=course_exp)
        for course_link in course_links:  # for every course link in the courses list page
            course_iid = course_exp.findall(course_link.attrs['href'])[0].split('=')[-1]
            courses[course_iid] = {
                'name': course_link.contents[0].text.strip(),
                'initial_year': None,
                'final_year': None,
                'abbreviation': None,
                'degree': None
            }

            # fetch the course curricular plan to find the activity years
            soup = request_to_soup(session.get(url_curricular_plans.format(institution, course_iid)))
            year_links = soup.find_all(href=re.compile("\\bano_lectivo=\\d+\\b"))
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
        for degree in degrees:
            soup = request_to_soup(session.get(url_statistics.format(institution, degree)))
            course_links = soup.find_all(href=course_exp)
            for course_link in course_links:
                course_iid = course_link.attrs['href'].split('=')[-1]
                abbr = course_link.contents[0]
                if course_iid in courses:
                    courses[course_iid]['abbreviation'] = abbr
                    courses[course_iid]['degree'] = degrees[degree]['id']
                else:
                    raise Exception("Course {}({}) was listed in the abbreviation list but wasn't found".format(
                        abbr, course_iid))

        # add to db
        for course_iid, course_info in courses.items():
            db_cursor.execute('SELECT name, initial_year, final_year, abbreviation, degree '
                              'FROM Courses '
                              'WHERE institution=? AND internal_id=?',
                              (institutions[institution]['id'], course_iid))
            exists = False
            different_time = False
            different_abbreviation = False
            different_degree = False
            for row in db_cursor.fetchall():
                course_name = row[0]
                course_initial_year = row[1]
                course_final_year = row[2]
                course_abbreviation = row[3]
                course_degree = row[4]

                if course_name != course_info['name']:
                    raise Exception("Different courses had an id collision")

                exists = True

                # course information changed (or previously unknown information appeared)
                if course_initial_year != course_info['initial_year'] or course_final_year != course_info['final_year']:
                    different_time = True

                if course_abbreviation != course_info['abbreviation']:
                    different_abbreviation = True

                if course_degree != course_info['degree']:
                    different_degree = True

            if not exists:  # unknown department, add it to the DB
                print("Adding  {}({}, {}) {} - {} to the database".format(
                    course_info['name'],
                    course_iid,
                    course_info['abbreviation'],
                    course_info['initial_year'],
                    course_info['final_year']))
                db_cursor.execute(
                    'INSERT INTO Courses'
                    '(internal_id, name, initial_year, final_year, abbreviation, degree, institution) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (course_iid,
                     course_info['name'],
                     course_info['initial_year'],
                     course_info['final_year'],
                     course_info['abbreviation'],
                     course_info['degree'],
                     institutions[institution]['id']))

            if different_time:  # update running date
                if course_info['initial_year'] is not None and course_info['final_year'] is not None:
                    print("Updating course {}({}) (now goes from {} to {})".format(
                        course_info['name'], course_iid, course_info['initial_year'], course_info['final_year']))
                    db_cursor.execute(
                        'UPDATE Courses '
                        'SET initial_year = ?, final_year=? '
                        'WHERE internal_id=? AND institution=?',
                        (course_info['initial_year'],
                         course_info['final_year'],
                         course_iid,
                         institutions[institution]['id']))

            if different_abbreviation:  # update abbreviation
                if course_info['abbreviation'] is not None:
                    print("Updating course {}({}) abbreviation to {}".format(
                        course_info['name'], course_iid, course_info['abbreviation']))
                    db_cursor.execute(
                        'UPDATE Courses '
                        'SET abbreviation=? '
                        'WHERE internal_id=? AND institution=?',
                        (course_info['abbreviation'], course_iid, institutions[institution]['id']))

            if different_degree:  # update degree
                if course_info['degree'] is not None:
                    print("Updating course {}({}) degree to {}".format(
                        course_info['name'], course_iid, course_info['degree']))
                    db_cursor.execute(
                        'UPDATE Courses '
                        'SET degree=? '
                        'WHERE internal_id=? AND institution=?',
                        (course_info['degree'], course_iid, institutions[institution]['id']))

        database.commit()
        print("Changes saved")


def request_to_soup(request):
    soup = BeautifulSoup(request.text, 'html.parser')
    # Take useless stuff out of the way for better debugging.
    # Also spend some time on it to avoid loading the server too much
    for tag in soup.find_all('script'):
        tag.decompose()
    for tag in soup.find_all('head'):
        tag.decompose()
    for tag in soup.find_all('img'):
        tag.decompose()
    for tag in soup.find_all('meta'):
        tag.decompose()
    if soup.find(type="password") is not None:
        print("Shit happened")
        exit()
    return soup


populate_departments(session, database)
populate_classes(session, database)
populate_courses(session, database)
