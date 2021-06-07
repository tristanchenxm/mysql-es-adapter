CREATE TABLE person
(
    id          INT PRIMARY KEY,
    `name`      VARCHAR(100) NOT NULL,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP()
);

CREATE TABLE person_info
(
    person_id   INT NOT NULL PRIMARY KEY,
    age         INT NOT NULL,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP()
);

CREATE TABLE person_job
(
    person_id INT NOT NULL PRIMARY KEY,
    employer  VARCHAR(255),
    job_title VARCHAR(100),
    salary    DECIMAL(10, 2)
);

CREATE TABLE `event`
(
    id          INT PRIMARY KEY,
    person_id   INT          NOT NULL,
    event_name  VARCHAR(128) NOT NULL,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP() ON UPDATE CURRENT_TIMESTAMP(),
    KEY (person_id)
);

CREATE TABLE relative
(
    id          INT PRIMARY KEY,
    person_id_1 INT         NOT NULL,
    person_id_2 INT         NOT NULL,
    relation    VARCHAR(32) NOT NULL,
    KEY (person_id_1),
    KEY (person_id_2)
);