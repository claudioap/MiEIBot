from bs4 import BeautifulSoup


def parse_clean_request(request):
    soup = BeautifulSoup(request.text, 'html.parser')
    # Take useless stuff out of the way for better debugging.
    for tag in soup.find_all('script'):
        tag.decompose()
    for tag in soup.find_all('head'):
        tag.decompose()
    for tag in soup.find_all('img'):
        tag.decompose()
    for tag in soup.find_all('meta'):
        tag.decompose()
    return soup


def abbr_to_course_iid(database, abbr, year=None):
    short_abbr = abbr.split('/')[0]

    if abbr in database.course_abbreviations:
        matches = database.course_abbreviations[abbr]
    elif short_abbr in database.course_abbreviations:
        matches = database.course_abbreviations[short_abbr]
    else:
        return None

    if len(matches) == 0:
        return None
    elif len(matches) == 1:
        return matches[0].identifier
    else:
        if year is None:
            raise Exception("Multiple matches. Year unspecified")

        for match in matches:
            if match.initial_year <= year <= match.last_year:
                return match.identifier


def weekday_to_id(database, weekday):
    if weekday in database.weekdays:
        return database.weekdays[weekday]

    for known_weekday in database.weekdays:
        if weekday.split('-')[0].lower() in known_weekday.lower():
            return database.weekdays[weekday]
