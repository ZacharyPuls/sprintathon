ALTER TYPE SUBMISSION_TYPE ADD VALUE 'BONUS';

DELETE FROM _VERSION;
INSERT INTO _VERSION(MAJOR, MINOR, PATCH) VALUES(1, 0, 3);