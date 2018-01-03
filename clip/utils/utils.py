from bs4 import BeautifulSoup

from clip import Database


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


def abbreviation_to_course(database: Database, abbreviation: str, year=None):
    short_abbr = abbreviation.split('/')[0]

    if abbreviation in database.course_abbreviations:
        matches = database.course_abbreviations[abbreviation]
    elif short_abbr in database.course_abbreviations:
        matches = database.course_abbreviations[short_abbr]
    else:
        return None

    if len(matches) == 0:
        return None
    elif len(matches) == 1:
        return matches[0]
    else:
        if year is None:
            raise Exception("Multiple matches. Year unspecified")

        for match in matches:
            if match.initial_year <= year <= match.last_year:
                return match


def weekday_to_id(database: Database, weekday: int):
    if weekday in database.weekdays:
        return database.weekdays[weekday]

    for known_weekday in database.weekdays:
        if weekday.split('-')[0].lower() in known_weekday.lower():
            return database.weekdays[weekday]


def get_month_periods(database: Database, month: int):
    result = []
    for period in database.periods:
        if period.start_month is None or period.end_month is None:
            continue

        year_changes = period.start_month > period.end_month
        if year_changes and (month >= period.end_month or month <= period.start_month):
            result.append(period)
            break

        if not year_changes and period.start_month <= month <= period.end_month:
            result.append(period)
            break

    return result
