-- Example migration file --
-- Any time a schema change is made from here on out (starting at v1.0.0), a migration file will need to be created --
-- Name the file patch_[major]-[minor]-[patch].sql, and include any commands needed to bring the schema up-to-date --
--     from the last patch file --

DELETE FROM _VERSION;
INSERT INTO _VERSION(MAJOR, MINOR, PATCH) VALUES(1, 0, 0);