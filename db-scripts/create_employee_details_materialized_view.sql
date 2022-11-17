CREATE MATERIALIZED VIEW employee_details_view
AS
SELECT e.din as din,
e.employeeid as employeeId,
e.firstname as firstName,
e.lastname as lastName,
e.ain as employeeAddress,
ea.ain as ain,
ea.city as city,
ea.district as district,
ea.pincode as pincode
FROM employees e RIGHT OUTER JOIN employeeaddresses ea
ON e.ain = ea.ain