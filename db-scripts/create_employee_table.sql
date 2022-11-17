CREATE TABLE public.employees(
din UUID DEFAULT gen_random_uuid() NOT NULL,
firstName varchar(50) DEFAULT NULL,
lastName varchar(50) DEFAULT NULL,
employeeId BIGINT DEFAULT NULL,
ain UUID DEFAULT NULL,
PRIMARY KEY(din),
CONSTRAINT fk_address
      FOREIGN KEY(ain)
	  REFERENCES employeeAddresses(ain)
)