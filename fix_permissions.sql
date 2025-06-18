-- Fix permissions for pbm_admin user
GRANT ALL ON SCHEMA public TO pbm_admin;
GRANT CREATE ON SCHEMA public TO pbm_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO pbm_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO pbm_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO pbm_admin;

-- Also grant usage on the schema
GRANT USAGE ON SCHEMA public TO pbm_admin; 