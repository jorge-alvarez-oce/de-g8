CREATE VIEW `gcp-data-engineer-project-001.business.vwVentas`
as  
select 
orderNumber,
orderDate as Fecha,
customerName,
concat(contactLastName,", ",contactFirstName) as Contacto,
cuscity,
cusstate,
cuscountry,
salesRepEmployeeNumber,
amount,
productCode,
quantityOrdered,
priceEach,
OrderLineNumber, 
productName,
productLine,
productScale,
productVendor,
productDescription,
--textDescription,
concat(tlastName,", ",firstName) as Vendedor,
firstName,
officeCode,
reportsTo,
jobTitle,
empcity as ciudad_oficina,
empstate as estado_oficina,
empcountry as pais_oficina,
empterritory as territorio_oficina,
emppostalcode as codigo_postal
 from 
`gcp-data-engineer-project-001.stagging.ventas`