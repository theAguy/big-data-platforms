use AdventureWorksLT2017
--1--
select CompanyName from salesLT.Customer
where FirstName = 'James' and MiddleName = 'D.' and LastName = 'Kramer'

--2--
select DISTINCT c.CompanyName 
from salesLT.Customer c
inner join SalesLT.CustomerAddress ca on c.CustomerID = ca.CustomerID
inner join SalesLT.Address a on ca.AddressID = a.AddressID
where City like 'D%'

--3--
select DISTINCT c.CompanyName, a.AddressLine1
from salesLT.CustomerAddress ca
left join SalesLT.Customer c on c.CustomerID = ca.CustomerID
left join SalesLT.Address a on ca.AddressID = a.AddressID 
where AddressType = 'Shipping'

--4--
select c.CompanyName, o.SubTotal, o.TaxAmt, o.Freight, o.TotalDue
from salesLT.Customer c
inner join SalesLT.SalesOrderHeader o on c.CustomerID = o.CustomerID
where o.TotalDue > 50000
order by o.TotalDue DESC

--5--
select count(*) 
from SalesLT.Address a
left join SalesLT.SalesOrderHeader o on a.AddressID = o.ShipToAddressID
left join SalesLT.SalesOrderDetail od on o.SalesOrderID = od.SalesOrderID
left join SalesLT.Product p on od.ProductID = p.ProductID
left join SalesLT.ProductCategory pc on p.ProductCategoryID = pc.ProductCategoryID
where a.City = 'London' and pc.Name = 'Cranksets'


--6--
SELECT A.ListPrice , A.Weight, A.Color, A.ProductID, B.ProductID
FROM SalesLT.Product A, SalesLT.Product B
where A.ListPrice = B.ListPrice AND A.Weight = B.Weight AND A.Color = B.Color and A.Name <> B.Name and A.ProductID < B.ProductID
order by A.ProductID, B.ProductID


--7--
select sum(od.OrderQty) as [count], avg(p.ListPrice) as [average]
from SalesLT.Product p 
inner join SalesLT.SalesOrderDetail od on p.ProductID = od.ProductID
where ListPrice > 1000

--8--
select pc.Name, sum(p.weight) as [sum_weights]
from SalesLT.ProductCategory pc
inner join SalesLT.Product p on pc.ProductCategoryID = p.ProductCategoryID
group by pc.Name
having  sum(p.weight) is not NULL
order by [sum_weights] DESC

--9--
select distinct pm.Name, count(p.ProductNumber) as [counter]
from SalesLT.Product p
left join SalesLT.ProductModel pm on p.ProductModelID = pm.ProductModelID
Group by pm.Name
having count(p.ProductNumber) >= 10

--10--
with result as (
select top 5 p.Name, sum(od.OrderQty)*p.ListPrice as [Total]
from SalesLT.SalesOrderDetail od
left join SalesLT.Product p on od.ProductID = p.ProductID
group by p.Name, p.ListPrice
order by [Total] DESC)
select * from result
order by result.Total ASC


--11--
select distinct pm.Name, count(p.Size) as [Size]
from SalesLT.Product p
left join SalesLT.ProductModel pm on p.ProductModelID = pm.ProductModelID
Where p.Size = 'XL' or p.Size = 'S'
group by pm.Name
having count(p.Size) > 1


--12--
select top 1 p.ListPrice, pd.Description
from SalesLT.Product p
left join SalesLT.ProductModelProductDescription pmpd on p.ProductModelID = pmpd.ProductModelID
left join SalesLT.ProductDescription pd on pmpd.ProductDescriptionID = pd.ProductDescriptionID
order by p.ListPrice ASC





