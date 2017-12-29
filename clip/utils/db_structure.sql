/* TODO:
   - Foreign keys & other constraints
   - Remove calculable fields (if their caching don't give that much of a performance benefit)
 */

/* Independent tables: */
CREATE TABLE "Degrees" (
  `id`          INTEGER PRIMARY KEY AUTOINCREMENT,
  `name_en`     TEXT,
  `name_pt`     TEXT NOT NULL,
  `internal_id` TEXT
);

CREATE TABLE "Periods" (
  `id`          INTEGER,
  `name`        TEXT    NOT NULL,
  `stage`       INTEGER NOT NULL,
  `stages`      INTEGER NOT NULL,
  `type_letter` TEXT,
  PRIMARY KEY (`id`)
);

CREATE TABLE `Teachers` (
  `id`   INTEGER,
  `name` INTEGER NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE "TurnTypes" (
  `id`   INTEGER,
  `abbr` TEXT NOT NULL,
  `type` TEXT NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE "Institutions" (
  id           INTEGER PRIMARY KEY,
  internal_id  TEXT,
  abbreviation TEXT NOT NULL,
  name         TEXT,
  initial_year INTEGER,
  last_year    INTEGER
);

CREATE TABLE "Buildings" (
  `id`   INTEGER,
  `name` INTEGER
);

CREATE TABLE "Weekdays" (
  `id`      INTEGER PRIMARY KEY AUTOINCREMENT,
  `name_en` TEXT,
  `name_pt` TEXT
);

/* Dependencies: Institutions */
CREATE TABLE "Departments" (
  `id`           INTEGER PRIMARY KEY AUTOINCREMENT,
  `internal_id`  TEXT UNIQUE,
  `name`         TEXT    NOT NULL,
  `institution`  INTEGER NOT NULL,
  `initial_year` INTEGER,
  `last_year`    INTEGER
);


/* Dependencies: Departments */
CREATE TABLE "Classes" (
  `id`          INTEGER PRIMARY KEY AUTOINCREMENT,
  `name`        TEXT NOT NULL,
  `internal_id` TEXT,
  `department`  INTEGER
);

/* Dependencies: Departments */
CREATE TABLE "Classrooms" (
  `id`       INTEGER,
  `name`     TEXT    NOT NULL,
  `building` INTEGER NOT NULL,
  PRIMARY KEY (`id`)
);

/* Dependencies: Departments, Degrees*/
CREATE TABLE "Courses" (
  `id`           INTEGER PRIMARY KEY AUTOINCREMENT,
  `abbreviation` TEXT,
  `name`         TEXT    NOT NULL,
  `institution`  INTEGER NOT NULL,
  `internal_id`  TEXT,
  `degree`       INTEGER,
  `initial_year` INTEGER,
  `final_year`   INTEGER
);

/* Dependencies: Courses, Institution (both optional, can be found through JOIN's) */
CREATE TABLE "Students" (
  `id`           INTEGER PRIMARY KEY AUTOINCREMENT,
  `name`         TEXT,
  `internal_id`  INTEGER,
  `abbreviation` TEXT,
  `course`       INTEGER,
  `institution`  INTEGER
);

/* Dependencies: Students, Courses */
CREATE TABLE "Admissions" (
  `id`         INTEGER PRIMARY KEY AUTOINCREMENT,
  `student_id` INTEGER, /* In case corresponding student can be found */
  `course_id`  INTEGER, /* Course this admission enabled */
  `phase`      INTEGER, /* One out of three admission phases */
  `year`       INTEGER,
  `option`     INTEGER,
  `state`      TEXT,
  `check_date` INTEGER,
  `name`       TEXT  /* In case there is no student to assign to*/
);

/* Dependencies: Classes */
CREATE TABLE "ClassInstances" (
  `id`     INTEGER PRIMARY KEY AUTOINCREMENT,
  `class`  INTEGER NOT NULL,
  `period` INTEGER NOT NULL,
  `year`   INTEGER NOT NULL,
  `regent` INTEGER
);

/* Dependencies: ClassInstances, TurnTypes */
CREATE TABLE "Turns" (
  `id`             INTEGER PRIMARY KEY AUTOINCREMENT,
  `number`         INTEGER NOT NULL,
  `type`           INTEGER NOT NULL,
  `class_instance` INTEGER NOT NULL,
  `hours`          INTEGER,
  `enrolled`       INTEGER,
  `capacity`       INTEGER,
  `routes`         TEXT,
  `restrictions`   TEXT,
  `state`          INTEGER
);

/* Dependencies: Turns, Classrooms */
CREATE TABLE "TurnInstances" (
  `turn`      INTEGER,
  `start`     INTEGER,
  `end`       INTEGER,
  `weekday`   INTEGER,
  `classroom` INTEGER,
  PRIMARY KEY (`turn`, `weekday`, `start`)
);

/* Dependencies: Turns, Students */
CREATE TABLE `TurnStudents` (
  `turn`    INTEGER,
  `student` INTEGER,
  PRIMARY KEY (`turn`, `student`)
);

/* Dependencies: Turns, Teachers */
CREATE TABLE `TurnTeachers` (
  `turn`    INTEGER,
  `teacher` INTEGER,
  PRIMARY KEY (`turn`, `teacher`)
);

/* Dependencies: Students, Classes */
CREATE TABLE "Enrollments" (
  `student_id`     INTEGER NOT NULL,
  `class_instance` INTEGER NOT NULL,
  `attempt`        INTEGER, /* Attempt that this enrollment constituted */
  `student_year`   INTEGER, /* The year/grade the student was considered to have as of this enrollment */
  `statutes`       TEXT, /* Statutes giving the student benefits on this enrollment */
  `observation`    TEXT, /* Unparsed information that would get discarded otherwise */
  PRIMARY KEY (`student_id`, `class_instance`)
);


CREATE VIEW 'ClassesComplete' AS
  SELECT
    Periods.stage             AS 'period',
    Periods.stages            AS 'total_periods',
    ClassInstances.year       AS 'year',
    Classes.internal_id       AS 'class_iid',
    Classes.name              AS 'class_name',
    Departments.internal_id   AS 'dept_iid',
    Departments.name          AS 'dept_name',
    Institutions.internal_id  AS 'inst_iid',
    Institutions.abbreviation AS 'inst_name'
  FROM ClassInstances
    JOIN Periods ON Periods.id = ClassInstances.period
    JOIN Classes ON ClassInstances.class = Classes.id
    JOIN Departments ON Classes.department = Departments.id
    JOIN Institutions ON Departments.institution = Institutions.id;

CREATE VIEW 'ClassInstancesComplete' AS
  SELECT
    ClassInstances.id        AS 'class_instance_id',
    Periods.stage            AS 'period_stage',
    Periods.type_letter      AS 'period_letter',
    ClassInstances.year      AS 'year',
    Classes.internal_id      AS 'class_iid',
    Departments.internal_id  AS 'department_iid',
    Institutions.internal_id AS 'institution_iid'
  FROM ClassInstances
    JOIN Periods ON Periods.id = ClassInstances.period
    JOIN Classes ON ClassInstances.class = Classes.id
    JOIN Departments ON Classes.department = Departments.id
    JOIN Institutions ON Departments.institution = Institutions.id;
