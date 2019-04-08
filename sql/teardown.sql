-- this file is not to be run in production:
-- it is provided to tear down the test environment, so that we can set it up
-- the same way each time.


DROP TYPE education_level CASCADE;
DROP TYPE vote_result CASCADE;
DROP TABLE weights CASCADE;
DROP TABLE dogs CASCADE;
DROP TABLE votes CASCADE;
DROP TABLE voters CASCADE;