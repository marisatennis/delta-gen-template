## Document Title: [Product Dimension Mapping]


## Details
 The below maps the journey of the Products stored in Dynamics 365 F&O from source to PowerBI. The purpose of this table is to store all the different types of Products used in Dynamics 365 F&O. A product can be either a Stock Inventory or a Service.


## Silver Layer Tables

### Products
[inventtable](./design/mapping/silverlayer/ecoresproduct.MD)

### Product Attribute
[ecoresproduct](./design/mapping/silverlayer/ecoresproduct.MD)

### Attribute Value
[ecoresproduct](./design/mapping/silverlayer/ecoresproduct.MD)


## Temp Tables

### Temp_packtype

Describe what this temp table does.

| Source System    | Source Table    | Source Fields | Data Type | IsNull? | DefaultValue |
|------------------|-----------------|---------------|-----------|---------|--------------|
| Dynamics 365 F&O | inventtable     | itemid        | STRING    | No      | Unknown      |
| Dynamics 365 F&O | inventable      | dataareaid    | STRING    | No      | -1           |
| Dynamics 365 F&O | ecorestextvalue | textvalue     | STRING    | No      | -1           |

### Temp Table Joins

| Join Type | From Table           | Join Table                        | Source Table Keys | Join Table Keys |
|-----------|----------------------|-----------------------------------|-------------------|-----------------|
| LEFT      | silver_inventtable   | silver_ecoresproduct              | product           | recid           |
| LEFT      | silver_ecoresproduct | silver_ecoresproductinstancevalue | recid             | product         |

### Filters

| Table                         | Filter Field | Filter        |
|-------------------------------|--------------|---------------|
| temp_packtype.ecoresattriubte | type         | = "Pack Type" |




## Gold Layer (Product Dimension)

#### Load Mode

- Incremental --> Watermark
- Full Load

| Source Table                    | Source Fields                   | Target Field          | Data Type | IsNull? | DefaultValue     | Partition Field | Model Field         | Type of Field |
|---------------------------------|---------------------------------|-----------------------|-----------|---------|------------------|-----------------|---------------------|---------------|
|                                 | UUID                            | ProductID             | STRING    | No      | -1               | No              | ProductID           | pk            |
| silver_inventtable              | CONCAT(dataareaid,itemid)       | ProductNaturalID      | STRING    | No      | Unknown          | No              |                     |               |
| silver_ecoresproducttranslation | name                            | Product               | STRING    | No      | Unknown          | No              | Product             | ref           |
| silver_inventtable              | itemtype                        | ProductType           | STRING    | No      | Unknown          | No              | Product Type        | ref           |
| silver_inventtable              | itemtype                        | ItemID                | STRING    | No      | Unknown          | No              | Product Type        | ref           |
| silver_inventtable              | netweight                       | NetWeight             | FLOAT     | No      | NULL             | No              | Net Weight          | ref           |
| silver_inventtable              | taraweight                      | TareWeight            | FLOAT     | No      | NULL             | No              | Tare Weight         | ref           |
| silver_ecorestextvalue          | textvalue                       | ProductPerPallet      | FLOAT     | No      | NULL             | No              | Product per Pallet  | ref           |
|                                 | if(name like '%CON:' True/False | ConsignmentFlag       | BOOLEAN   | No      | False            | No              | ConsignmentFlag     | ref           |
| silver_inventtable              | primaryvendorid                 | SupplierNaturalID     | STRING    | Yes     | Unknown          | No              |                     |               |
| silver_inventtable              | defaultdimension                | BusinessUnitNaturalID | STRING    |         | Unknown          | No              |                     |               |
| silver_MBSProductGUID           | CONCAT(dataareaid,custaccount)  | CustomerNaturalID     | STRING    | Yes     | Unknown          | No              |                     |               |
| silver_custvendexternalitem     | externalitemid                  | ExternalItemID        | STRING    | Yes     | Unknown          | No              | External Item ID    |               |
| silver_custvendexternalitem     | externalitemtxt                 | ExternalDescription   | STRING    | Yes     | Unknown          | No              | External Desciption |               |
| silver_custvendexternalitem     | textvalue                       | PackType              | STRING    | Yes     | Unknown          | No              | Pack Type           |               |
| silver_inventtable              | IsDelete                        | IsDelete              | BOOLEAN   | No      | false            | No              |                     |               |
| silver_inventtable              | IsDelete                        | ModifiedOn            | DATETIME  | No      | 1900-01-01 00:00 | No              |                     |               |
| silver_inventtable              | IsDelete                        | CreatedOnYear         | INT       | No      | 1900             | Yes             |                     |               |
| silver_inventtable              | IsDelete                        | CreatedOnMonth        | INT       | No      | 1                | Yes             |                     |               |

### Joins

| Join Type | From Table                        | Join Table                        | Source Table Keys         | Join Table Keys           |
|-----------|-----------------------------------|-----------------------------------|---------------------------|---------------------------|
| LEFT      |                                   | silver_ecoresproduct              | product                   | recid                     |
| LEFT      | silver_ecoresproduct              | silver_ecoresproductinstancevalue | recid                     | product                   |
| LEFT      | silver_ecoresproductinstancevalue | silver_ecoresattributeValue       | recid                     | instancevalue             |
| LEFT      | silver_ecoresattributeValue       | silver_EcoResAttribute            | Attribute                 | recid                     |
| LEFT      | silver_ecoresattributeValue       | silver_ecorestextvalue            | Value                     | recid                     |
| LEFT      | silver_inventtable                | silver_ecoresproducttranslation   | product                   | product                   |
| LEFT      | silver_inventtable                | silver_MBSProductGUID             | CONCAT(dataareaid,itemid) | CONCAT(dataareaid,itemid) |
| LEFT      | silver_custventexternalitem       | silver_inventtable                | CONCAT(dataareaid,itemid) | CONCAT(dataareaid,itemid) |
| LEFT      | Temp_packtype                     | silver_inventtable                | CONCAT(dataareaid,itemid) | CONCAT(dataareaid,itemid) |


### Filters

| Table                         | Filter Field | Filter                |
|-------------------------------|--------------|-----------------------|
| silver_ecoresattribute        | name         | = "Number per Pallet" |
| silver_mbsproductguid         | type         | = 0                   |
| temp_packtype.ecoresattriubte | type         | = "Pack Type"         |

