CREATE TABLE public.employeeAddresses(
ain UUID DEFAULT gen_random_uuid() NOT NULL,
city varchar(50) DEFAULT NULL,
district varchar(255) DEFAULT NULL,
pinCode varchar(50) DEFAULT NULL,
PRIMARY KEY (ain)
)