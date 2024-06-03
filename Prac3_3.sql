CREATE DATABASE testspark;

USE testspark;

CREATE TABLE employee (
  id INT PRIMARY KEY,
  name VARCHAR(50),
  gender CHAR(1),
  age INT
);

INSERT INTO employee (id, name, gender, age) VALUES
(1, 'Alice', 'F', 22),
(2, 'John', 'M', 25);
