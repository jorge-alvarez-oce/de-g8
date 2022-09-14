truncate table `gcp-data-engineer-project-001.stagging.ventas`;

insert into `gcp-data-engineer-project-001.stagging.ventas`
SELECT tor.orderNumber,tor.orderDate,tor.customerNumber,
tcus.customerName,tcus.contactLastName,tcus.contactFirstName,tcus.city,tcus.state,tcus.country,tcus.salesRepEmployeeNumber,
tpay.amount,
tdet.productCode,tdet.quantityOrdered,tdet.priceEach,tdet.OrderLineNumber, 
tprod.productName,tprod.productLine,tprod.productScale,tprod.productVendor,tprod.productDescription,
tlines.textDescription,
temp.lastName,temp.firstName,temp.officeCode,temp.reportsTo,temp.jobTitle,
tofi.city,tofi.state,tofi.country,tofi.territory,tofi.postalcode
FROM `gcp-data-engineer-project-001.access.orders` tor
left outer join `gcp-data-engineer-project-001.access.customers` tcus on tor.customerNumber= tcus.customerNumber
left outer join `gcp-data-engineer-project-001.access.payments` tpay on  tcus.customerNumber=tpay.customerNumber
left outer join `gcp-data-engineer-project-001.access.orderdetails` tdet on tor.orderNumber=tdet.orderNumber
left outer join `gcp-data-engineer-project-001.access.products` tprod on tdet.productCode=tprod.productCode
left outer join `gcp-data-engineer-project-001.access.productlines` tlines on tprod.productLine=tlines.productLine
left outer join `gcp-data-engineer-project-001.access.employees` temp on tcus.salesRepEmployeeNumber=temp.employeeNumber
left outer join`gcp-data-engineer-project-001.access.offices`  tofi on temp.officeCode=tofi.officeCode;