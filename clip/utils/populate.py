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
    institutions[row[0]] = {
        'id': row[1],
        'name': row[2],
        'departments': []
    }

session = Session()


# Check institution years of existence
def institution_years(session, institution):
    institution_years = []
    soup = request_to_soup(
        session.get('https://clip.unl.pt/'
                    'utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo?institui%E7%E3o='
                    + str(institution)))
    year = re.compile("\\b\\d{4}\\b")
    elements = soup.find_all(href=year)
    for element in elements:
        match = year.findall(element.attrs['href'])[0]
        institution_years.append(match)
    return institution_years


def populate_departments(session, database):
    db_cursor = database.cursor()
    departments = {}  # internal_id -> name
    for institution in institutions:
        # Fetch institution ID from DB
        db_cursor.execute('SELECT id FROM Institutions WHERE internal_id=?', (int(institution),))
        institution_row = db_cursor.fetchone()
        if institution_row is None or len(institution_row) == 0:
            raise Exception("Attempted to assign a department to an unknown institution")
        institution_id = institution_row[0]

        # Find institution years of activity
        years = institution_years(session, institution)
        for year in years:  # Find the departments that existed under each year
            soup = request_to_soup(session.get("https://clip.unl.pt/"
                                               "utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo"
                                               "?ano_lectivo={}&institui%E7%E3o={}".format(year, institution)))
            department_exp = re.compile("\\bsector=\\d+\\b")
            department_links = soup.find_all(href=department_exp)
            for department_link in department_links:
                match = department_exp.findall(department_link.attrs['href'])[0]
                department_id = str(match).split('=')[1]
                department_name = department_link.contents[0]

                if department_id in departments:  # update creation year
                    old = departments[department_id]
                    departments[department_id] = (old[0], year, old[2])
                    print("Department {} now goes from {} to {}".format(old[0], year, old[2]))
                else:  # insert new
                    print("Found: {}({}) in {}".format(department_name, department_id, year))
                    departments[department_id] = (department_name, year, year)  # (name, creation, last year)

        # Add departments to the database
        for department_id, department_info in departments.items():
            db_cursor.execute('SELECT name, creation, last_year '
                              'FROM Departments WHERE institution=? AND internal_id=?',
                              (institution_id, int(department_id)))
            exists = False
            different = False
            for row in db_cursor.fetchall():
                if row[0] != department_info[0]:
                    raise Exception("Different departments had an id collision")

                #   creation date the same and end date the same
                elif row[1] != int(department_info[1]) or row[2] != int(department_info[2]):
                    different = True
                    exists = True

            if not exists:  # Unknown department, add it to the DB
                print("Adding  {}({}) {} - {} to the database".format(
                    department_info[0], department_id, department_info[1], department_info[2]))
                db_cursor.execute(
                    'INSERT INTO Departments(internal_id, name, creation, last_year, institution) '
                    'VALUES (?, ?, ?, ?, ?)',
                    (department_id, department_info[0], int(department_info[1]), int(department_info[2]),
                     institution_id))

            if different:
                print("Updating department {} now goes from {} to {}".format(
                    department_info[0], department_info[1], department_info[2]))
                db_cursor.execute('UPDATE Departments SET (creation, last_year) = (?, ?) '
                                  'WHERE internal_id=?',
                                  (int(department_info[1]), int(department_info[2]), department_info[0]))

        database.commit()
        print("Changes saved")


def populate_courses(session, database):
    db_cursor = database.cursor()
    db_cursor.execute('SELECT Departments.id, Departments.internal_id, Institutions.internal_id, '
                      'Departments.creation, Departments.last_year '
                      'FROM Departments JOIN Institutions '
                      'WHERE Departments.institution = Institutions.id')

    for row in db_cursor.fetchall():
        department_id = row[0]
        department_iid = row[1]
        institution_iid = row[2]
        department_creation = row[3]
        department_last_year = row[4]
        classes = {}
        class_instances = {}
        period_ids = {}  # cache with [stages][stage] -> id

        # for each year this department operated
        for year in range(department_creation, department_last_year + 1):
            soup = request_to_soup(session.get("https://clip.unl.pt/"
                                               "utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo/sector"
                                               "?institui%E7%E3o={}&ano_lectivo={}&sector={}"
                                               "".format(institution_iid, year, department_iid)))
            period_links = soup.find_all(href=re.compile("\\bper%EDodo_lectivo=\\d\\b"))

            # for each period from this department
            for period_link in period_links:
                parts = period_link.attrs['href'].split('&')
                stage = parts[-1].split('=')[1]
                stages_letter = parts[-2].split('=')[1]
                stages = None
                if stages_letter == 't':
                    stages = 3
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
                    session.get("https://clip.unl.pt/"
                                "utente/institui%E7%E3o_sede/unidade_organica/ensino/ano_lectivo/sector/ano_lectivo"
                                "?tipo_de_per%EDodo_lectivo={}&sector={}&ano_lectivo={}"
                                "&per%EDodo_lectivo={}&institui%E7%E3o={}"
                                "".format(stages_letter, department_iid, year, stage, institution_iid)))

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
                    if class_iid not in class_instances:
                        class_instances[class_iid] = []

                    class_instances[class_iid].append({
                        'period': period_id,
                        'year': year
                    })

        # Add class to the database
        for class_iid, class_info in classes.items():
            db_cursor.execute(
                'SELECT name FROM Classes WHERE internal_id=? AND department=?',
                (class_iid, int(class_info['department'])))

            exists = False
            for row in db_cursor.fetchall():
                if row[0] != class_info['name']:
                    raise Exception("Different classes had an id collision {} with {}, id was {}".format(
                        class_info['name'], row[0], class_iid
                    ))
                else:
                    print("Class already known: {}({})".format(class_info['name'], class_iid))
                    exists = True

            if not exists:  # Unknown class, add it to the DB
                print("Adding  {}({}) to the database".format(class_info['name'], class_iid))
                db_cursor.execute('INSERT INTO Classes(internal_id, name, department) VALUES (?, ?, ?)',
                                  (class_iid, class_info['name'], class_info['department']))

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


# populate_departments(session, database)
populate_courses(session, database)
