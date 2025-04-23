-- Department
CREATE TABLE Department (
    deptID      VARCHAR(10)  PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    location    VARCHAR(100)
    /* mainOffice added later after Office exists */
) ENGINE = InnoDB;

-- Office
CREATE TABLE Office (
    oID       VARCHAR(10)  PRIMARY KEY,
    capacity  INT UNSIGNED NOT NULL CHECK (capacity > 0),

    -- (1-to-1) back-reference to Department; UNIQUE enforces “one office ↔ one dept”
    deptID    VARCHAR(10)  UNIQUE,
    CONSTRAINT fk_office_dept
        FOREIGN KEY (deptID) REFERENCES Department(deptID)
        ON DELETE SET NULL            -- if a dept is removed, keep the orphan office
        ON UPDATE CASCADE
) ENGINE = InnoDB;

-- Add mainOffice to Department
ALTER TABLE Department
    ADD COLUMN mainOffice VARCHAR(10) NOT NULL UNIQUE,
    ADD CONSTRAINT fk_dept_mainOffice
        FOREIGN KEY (mainOffice) REFERENCES Office(oID)
        ON DELETE RESTRICT
        ON UPDATE CASCADE;

-- Employee
CREATE TABLE Employee (
    empID     VARCHAR(10)  PRIMARY KEY,
    fullName  VARCHAR(100) NOT NULL,
    salary    DECIMAL(10,2) CHECK (salary >= 0),

    -- Works_for (N-side)
    deptID    VARCHAR(10)  NOT NULL,
    CONSTRAINT fk_emp_dept
        FOREIGN KEY (deptID) REFERENCES Department(deptID)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    -- Hosted_in 1-to-1 : one office, one employee
    office    VARCHAR(10)  NOT NULL UNIQUE,
    CONSTRAINT fk_emp_office
        FOREIGN KEY (office) REFERENCES Office(oID)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
) ENGINE = InnoDB;

-- EmployeeTask (for multi-valued attribute “tasks”)
CREATE TABLE EmployeeTask (
    empID  VARCHAR(10)  NOT NULL,
    task   VARCHAR(100) NOT NULL,

    PRIMARY KEY (empID, task),
    CONSTRAINT fk_task_emp
        FOREIGN KEY (empID) REFERENCES Employee(empID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE = InnoDB;
