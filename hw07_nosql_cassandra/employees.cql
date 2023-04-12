CREATE KEYSPACE hw07 WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' };

use hw07;

CREATE TABLE IF NOT EXISTS employees (
    employeeId uuid,
    firstName TEXT,
    lastName TEXT,
    officeId TEXT,
    phoneNumbers SET<TEXT>,
    supervisorName TEXT,
    primary key (employeeId, officeId)
);

BEGIN BATCH
INSERT INTO employees (employeeId, firstName, lastName, officeId, phoneNumbers, supervisorName)
VALUES (uuid(), 'Manny', 'Aboah', '0056', {'572-125-8898', '804-569-2235', '703-895-4563'}, 'Sherry Williams')
INSERT INTO employees (employeeId, firstName, lastName, officeId, phoneNumbers, supervisorName)
VALUES (uuid(), 'Allissa', 'Abdelwahed', '0057', {'571-145-6698', '804-339-2555', '703-892-4565'}, 'Priya Singh')
INSERT INTO employees (employeeId, firstName, lastName, officeId, phoneNumbers, supervisorName)
VALUES (uuid(), 'Johny', 'Quest', '0056', {'571-125-8899', '804-222-3355', '703-333-1111'}, 'Joe Buck')
INSERT INTO employees (employeeId, firstName, lastName, officeId, phoneNumbers, supervisorName)
VALUES (uuid(), 'Danny', 'Boyd', '0056', {'572-265-6969', '804-555-1232', '703-888-0036'}, 'Tucker Tank')
INSERT INTO employees (employeeId, firstName, lastName, officeId, phoneNumbers, supervisorName)
VALUES (uuid(), 'Rebel', 'Wilson', '0057', {'572-777-8898', '804-111-2235', '703-123-2244'}, 'Sherry Williams')
APPLY BATCH;

CREATE TABLE IF NOT EXISTS offices (
    officeName TEXT,
    city TEXT,
    streetAddress TEXT,
    officeId TEXT,
    primary key (officeId, officeName)
);

BEGIN BATCH
INSERT INTO offices (officeName, city, streetAddress, officeId)
VALUES ('Deloitte Rosslyn', 'Rosslyn', '1777 N Kent St, Arlington, VA 22209', '0056')
INSERT INTO offices (officeName, city, streetAddress, officeId)
VALUES ('AMZ-10', 'Reston', '1777 Worldgate Dr. Reston, VA 20171', '0057')
INSERT INTO offices (officeName, city, streetAddress, officeId)
VALUES ('Vaco', 'Falls Church', '2980 Fairview Park Dr, Falls Church, VA 22042', '0058')
INSERT INTO offices (officeName, city, streetAddress, officeId)
VALUES ('Deloitte Tysons', 'Tysons Corner', '7900 Tysons One Pl #800, McLean, VA 22102', '0059')
APPLY BATCH;