CREATE ROLE Administrator;
GRANT ALL ON ALL TABLES TO Administrator;

CREATE ROLE Visitor;
GRANT SELECT ON ALL TABLES TO Visitor;