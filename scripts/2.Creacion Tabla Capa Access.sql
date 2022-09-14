CREATE OR REPLACE TABLE `gcp-data-engineer-project-001.access.productlines`
(
  productLine STRING,
  textDescription STRING,
  htmlDescription STRING,
  image STRING
);

CREATE OR REPLACE TABLE  `gcp-data-engineer-project-001.access.orderdetails`
(
  orderNumber NUMERIC,
  productCode STRING,
  quantityOrdered NUMERIC,
  priceEach NUMERIC,
  orderLineNumber NUMERIC
);

CREATE OR REPLACE TABLE  `gcp-data-engineer-project-001.access.offices`
(
  officeCode STRING,
  city STRING,
  phone STRING,
  addressLine1 STRING,
  addressLine2 STRING,
  state STRING,
  country STRING,
  postalCode STRING,
  territory STRING
);

CREATE OR REPLACE TABLE  `gcp-data-engineer-project-001.access.products`
(
  productCode STRING,
  productName STRING,
  productLine STRING,
  productScale STRING,
  productVendor STRING,
  productDescription STRING,
  quantityInStock NUMERIC,
  buyPrice NUMERIC,
  MSRP NUMERIC
);

CREATE OR REPLACE TABLE  `gcp-data-engineer-project-001.access.employees`
(
  employeeNumber NUMERIC,
  lastName STRING,
  firstName STRING,
  extension STRING,
  email STRING,
  officeCode STRING,
  reportsTo NUMERIC,
  jobTitle STRING
);

CREATE OR REPLACE TABLE  `gcp-data-engineer-project-001.access.orders`
(
  orderNumber NUMERIC,
  orderDate DATE,
  requiredDate DATE,
  shippedDate DATE,
  status STRING,
  comments STRING,
  customerNumber NUMERIC
);

CREATE OR REPLACE TABLE `gcp-data-engineer-project-001.access.payments`
(
  customerNumber NUMERIC,
  checkNumber STRING,
  paymentDate DATE,
  amount NUMERIC
);

CREATE OR REPLACE TABLE  `gcp-data-engineer-project-001.access.customers`
(
  customerNumber NUMERIC,
  customerName STRING,
  contactLastName STRING,
  contactFirstName STRING,
  phone STRING,
  addressLine1 STRING,
  addressLine2 STRING,
  city STRING,
  state STRING,
  postalCode STRING,
  country STRING,
  salesRepEmployeeNumber NUMERIC,
  creditLimit NUMERIC
);