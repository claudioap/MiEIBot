/* TODO:
   - Foreign keys & other constraints
   - Remove calculable fields (if their caching don't give that much of a performance benefit)
 */

BEGIN TRANSACTION;
/* Independent tables: */
CREATE TABLE IF NOT EXISTS "Degrees" (
  `id`          INTEGER PRIMARY KEY AUTOINCREMENT,
  `name_en`     TEXT,
  `name_pt`     TEXT NOT NULL,
  `internal_id` TEXT
);

CREATE TABLE IF NOT EXISTS `Periods` (
  `id`          INTEGER,
  `name`        TEXT    NOT NULL,
  `stage`       INTEGER NOT NULL,
  `stages`      INTEGER NOT NULL,
  `type_letter` TEXT    NOT NULL,
  `start_month` INTEGER,
  `end_month`   INTEGER,
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `Teachers` (
  `id`   INTEGER,
  `name` INTEGER NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS "TurnTypes" (
  `id`   INTEGER,
  `abbr` TEXT NOT NULL,
  `type` TEXT NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE IF NOT EXISTS `Institutions` (
  `id`           INTEGER PRIMARY KEY AUTOINCREMENT,
  `internal_id`  TEXT,
  `abbreviation` TEXT NOT NULL,
  `name`         TEXT,
  `initial_year` INTEGER,
  `last_year`    INTEGER
);


CREATE TABLE IF NOT EXISTS "Buildings" (
  `id`          INTEGER PRIMARY KEY AUTOINCREMENT,
  `name`        TEXT NOT NULL,
  `institution` INTEGER
);

CREATE TABLE IF NOT EXISTS "Weekdays" (
  `id`      INTEGER PRIMARY KEY AUTOINCREMENT,
  `name_en` TEXT,
  `name_pt` TEXT
);


/* Dependencies: Institutions */
CREATE TABLE IF NOT EXISTS "Departments" (
  `id`           INTEGER PRIMARY KEY AUTOINCREMENT,
  `internal_id`  TEXT UNIQUE,
  `name`         TEXT    NOT NULL,
  `institution`  INTEGER NOT NULL,
  `initial_year` INTEGER,
  `last_year`    INTEGER
);


/* Dependencies: Departments */
CREATE TABLE IF NOT EXISTS "Classes" (
  `id`          INTEGER PRIMARY KEY AUTOINCREMENT,
  `name`        TEXT NOT NULL,
  `internal_id` TEXT,
  `department`  INTEGER
);

/* Dependencies: Departments */
CREATE TABLE IF NOT EXISTS "Classrooms" (
  `id`       INTEGER,
  `name`     TEXT    NOT NULL,
  `building` INTEGER NOT NULL,
  PRIMARY KEY (`id`)
);

/* Dependencies: Departments, Degrees*/
CREATE TABLE IF NOT EXISTS "Courses" (
  `id`           INTEGER PRIMARY KEY AUTOINCREMENT,
  `abbreviation` TEXT,
  `name`         TEXT    NOT NULL,
  `institution`  INTEGER NOT NULL,
  `internal_id`  TEXT,
  `degree`       INTEGER,
  `initial_year` INTEGER,
  `last_year`    INTEGER
);

/* Dependencies: Courses, Institution (both optional, can be found through JOIN's) */
CREATE TABLE IF NOT EXISTS "Students" (
  `id`           INTEGER PRIMARY KEY AUTOINCREMENT,
  `name`         TEXT,
  `internal_id`  INTEGER,
  `abbreviation` TEXT,
  `course`       INTEGER,
  `institution`  INTEGER
);

/* Dependencies: Students, Courses */
CREATE TABLE IF NOT EXISTS "Admissions" (
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
CREATE TABLE IF NOT EXISTS "ClassInstances" (
  `id`     INTEGER PRIMARY KEY AUTOINCREMENT,
  `class`  INTEGER NOT NULL,
  `period` INTEGER NOT NULL,
  `year`   INTEGER NOT NULL,
  `regent` INTEGER
);

/* Dependencies: ClassInstances, TurnTypes */
CREATE TABLE IF NOT EXISTS "Turns" (
  `id`             INTEGER PRIMARY KEY AUTOINCREMENT,
  `number`         INTEGER NOT NULL,
  `type`           INTEGER NOT NULL,
  `class_instance` INTEGER NOT NULL,
  `minutes`        INTEGER,
  `enrolled`       INTEGER,
  `capacity`       INTEGER,
  `routes`         TEXT,
  `restrictions`   TEXT,
  `state`          TEXT
);

/* Dependencies: Turns, Classrooms */
CREATE TABLE IF NOT EXISTS "TurnInstances" (
  `turn`      INTEGER,
  `start`     INTEGER,
  `end`       INTEGER,
  `weekday`   INTEGER,
  `classroom` INTEGER,
  PRIMARY KEY (`turn`, `weekday`, `start`)
);

/* Dependencies: Turns, Students */
CREATE TABLE IF NOT EXISTS `TurnStudents` (
  `turn`    INTEGER,
  `student` INTEGER,
  PRIMARY KEY (`turn`, `student`)
);

/* Dependencies: Turns, Teachers */
CREATE TABLE IF NOT EXISTS `TurnTeachers` (
  `turn`    INTEGER,
  `teacher` INTEGER,
  PRIMARY KEY (`turn`, `teacher`)
);

/* Dependencies: Students, Classes */
CREATE TABLE IF NOT EXISTS "Enrollments" (
  `student_id`     INTEGER NOT NULL,
  `class_instance` INTEGER NOT NULL,
  `attempt`        INTEGER, /* Attempt that this enrollment constituted */
  `student_year`   INTEGER, /* The year/grade the student was considered to have as of this enrollment */
  `statutes`       TEXT, /* Statutes giving the student benefits on this enrollment */
  `observation`    TEXT, /* Unparsed information that would get discarded otherwise */
  PRIMARY KEY (`student_id`, `class_instance`)
);


INSERT INTO `Weekdays` VALUES (1, 'Monday', 'Segunda-Feira');
INSERT INTO `Weekdays` VALUES (2, 'Tuesday', 'Terça-Feira');
INSERT INTO `Weekdays` VALUES (3, 'Wednesday', 'Quarta-Feira');
INSERT INTO `Weekdays` VALUES (4, 'Thursday', 'Quinta-Feira');
INSERT INTO `Weekdays` VALUES (5, 'Friday', 'Sexta-Feira');
INSERT INTO `Weekdays` VALUES (6, 'Saturday', 'Sábado');
INSERT INTO `Weekdays` VALUES (7, 'Sunday', 'Domingo');

INSERT INTO `TurnTypes` VALUES (1, 't', 'Theoretical');
INSERT INTO `TurnTypes` VALUES (2, 'p', 'Practical');
INSERT INTO `TurnTypes` VALUES (3, 'tp', 'Practical-Theoretical');
INSERT INTO `TurnTypes` VALUES (4, 'ot', 'Tutorial Orientation');
INSERT INTO `TurnTypes` VALUES (5, 's', 'Seminar');

INSERT INTO `Periods` VALUES (1, 'Yearly', 1, 1, 'a', 9, 7);
INSERT INTO `Periods` VALUES (2, '1st semester', 1, 2, 's', 9, 12);
INSERT INTO `Periods` VALUES (3, '2nd semester', 2, 2, 's', 3, 7);
INSERT INTO `Periods` VALUES (4, '1st trimester', 1, 4, 't', 9, 12);
INSERT INTO `Periods` VALUES (5, '2nd trimester', 2, 4, 't', 1, 3);
INSERT INTO `Periods` VALUES (6, '3rd trimester', 3, 4, 't', 4, 7);
INSERT INTO `Periods` VALUES (7, '4th trimester', 4, 4, 't', NULL, NULL);

INSERT INTO `Degrees` VALUES (1, 'Bachelor', 'Licenciatura', 'L');
INSERT INTO `Degrees` VALUES (2, 'Master', 'Mestrado', 'M');
INSERT INTO `Degrees` VALUES (3, 'PhD', 'Dotoramento', 'D');
INSERT INTO `Degrees` VALUES (4, 'Integrated Master', 'Mestrado Integrado', 'M');
INSERT INTO `Degrees` VALUES (5, 'Postgraduate', 'Pos-Graduação', 'Pg');
INSERT INTO `Degrees` VALUES (6, 'Advanced Studies', 'Estudos Avançados', 'EA');
INSERT INTO `Degrees` VALUES (7, 'Pre Graduation', 'Pré-Graduação', 'pG');
INSERT INTO `Degrees` VALUES (8, 'Curso Técnico Superior Profissional', 'Curso Técnico Superior Profissional', NULL);


CREATE VIEW IF NOT EXISTS 'ClassesComplete' AS
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

CREATE VIEW IF NOT EXISTS 'ClassInstancesComplete' AS
  SELECT
    ClassInstances.id        AS 'class_instance_id',
    Periods.id               AS 'period_id',
    ClassInstances.year      AS 'year',
    Classes.internal_id      AS 'class_iid',
    Classes.id               AS 'class_id',
    Classes.name             AS 'class_name',
    Departments.internal_id  AS 'department_iid',
    Institutions.internal_id AS 'institution_iid'
  FROM ClassInstances
    JOIN Periods ON Periods.id = ClassInstances.period
    JOIN Classes ON ClassInstances.class = Classes.id
    JOIN Departments ON Classes.department = Departments.id
    JOIN Institutions ON Departments.institution = Institutions.id;

COMMIT;