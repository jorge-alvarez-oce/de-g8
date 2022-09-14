truncate table `gcp-data-engineer-project-001.access.productlines`;
truncate table `gcp-data-engineer-project-001.access.orderdetails`;
truncate table `gcp-data-engineer-project-001.access.offices`;
truncate table `gcp-data-engineer-project-001.access.products`;
truncate table `gcp-data-engineer-project-001.access.employees`;
truncate table `gcp-data-engineer-project-001.access.orders`;
truncate table `gcp-data-engineer-project-001.access.payments`;
truncate table `gcp-data-engineer-project-001.access.customers`;


insert into  `gcp-data-engineer-project-001.access.productlines`
select productLine,
  textDescription,
  htmlDescription,
  image 
  from `gcp-data-engineer-project-001.quality.productlines`;

insert into `gcp-data-engineer-project-001.access.orderdetails`
select  orderNumber,
  productCode,
  quantityOrdered,
  priceEach,
  orderLineNumber 
  from `gcp-data-engineer-project-001.quality.orderdetails`;

insert into `gcp-data-engineer-project-001.access.offices`
select  officeCode,
  city,
  phone,
  addressLine1,
  addressLine2,
  state,
  country,
  postalCode,
  territory 
  from `gcp-data-engineer-project-001.quality.offices`;

insert into `gcp-data-engineer-project-001.access.products`
select productCode,
  productName,
  productLine,
  productScale,
  productVendor,
  productDescription,
  quantityInStock,
  buyPrice,
  MSRP 
  from `gcp-data-engineer-project-001.quality.products`;

insert into `gcp-data-engineer-project-001.access.employees`
select employeeNumber,
  lastName,
  firstName,
  extension,
  email,
  officeCode,
  reportsTo,
  jobTitle 
  from `gcp-data-engineer-project-001.quality.employees`;

insert into `gcp-data-engineer-project-001.access.orders`
select orderNumber,
  orderDate,
  requiredDate,
  shippedDate,
  status,
  comments,
  customerNumber 
 from `gcp-data-engineer-project-001.quality.orders`;

insert into `gcp-data-engineer-project-001.access.payments` 
select customerNumber,
  checkNumber,
  paymentDate,
  amount 
  from `gcp-data-engineer-project-001.quality.payments`;

insert into `gcp-data-engineer-project-001.access.customers`
select 
customerNumber,
  customerName,
  contactLastName,
  contactFirstName,
  phone,
  addressLine1,
  addressLine2,
  city,
  state,
  postalCode,
  country,
  salesRepEmployeeNumber,
  creditLimit 
  from `gcp-data-engineer-project-001.quality.customers`