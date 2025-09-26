
-- CREATE DATABASE DataWarehouse CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- Select it for subsequent statements
USE DataWarehouse;


-- Creating new schemas, check if main DB exists
*/

DROP DATABASE IF EXISTS DataWarehouse;

-- Create a fresh empty database
CREATE DATABASE DataWarehouse;


CREATE DATABASE DataWarehouse;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
