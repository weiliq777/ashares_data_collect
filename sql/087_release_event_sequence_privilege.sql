-- The release audit trigger allocates event_id values while the application
-- role transitions a data release.  Grant only sequence access; table access
-- remains governed separately.
GRANT USAGE, SELECT ON SEQUENCE ops.data_release_event_event_id_seq TO ashares_app;
