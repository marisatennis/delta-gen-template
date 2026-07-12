## Document Title: [ecoresproduct Mapping]


## Details
 Describe the details of the table, what information it contains.


## silver layer table

Please note that the source table and source field match the bronze layer naming conventions

### silver_souecesystem_tablename

#### Load Mode

- Incremental --> Watermark
- Full Load

| Source System    | Source Table | Source Fields    | Data Type | IsNull? | DefaultValue         | Partition Field |
|------------------|--------------|------------------|-----------|---------|----------------------|-----------------|
| Dynamics 365 F&O | inventtable  | itemid           | STRING    | No      | Unknown              | No              |
| Dynamics 365 F&O | inventtable  | product          | BIGINT    | No      | NULL                 | No              |
| Dynamcis 365 F&O | inventtable  | itemtype         | STRING    | No      | Unknown              | No              |
| Dynaimcs 365 F&O | inventtable  | netweight        | FLOAT     | No      | NULL                 | No              |
| Dynamics 365 F&O | inventtable  | taraweight       | FLOAT     | No      | NULL                 | No              |
| Dynamics 365 F&O | inventtable  | bomunitid        | STRING    | No      | Unknown              | No              |
| Dynamics 365 F&O | inventtable  | primaryvendorid  | STRING    | Yes     | NULL                 | No              |
| DYnamics 365 F&O | inventtable  | defaultdimension | BIGINT    | Yes     | NULL                 | No              |
| Dynamics 365 F&O | inventtable  | modifiedon       | TIMESTAMP | No      | 01/01/1900T00:00:00Z | No              |
| Dynamics 365 F&O | inventtable  | createdon        | TIMESTAMP | No      | 01/01/1900T00:00:00Z | No              |
| Dynamics 365 F&O | inventtable  | createdonyear    | TIMESTAMP | No      | 01/01/1900T00:00:00Z | Yes             |
| Dynamics 365 F&O | inventtable  | createdonmonth   | TIMESTAMP | No      | 01/01/1900T00:00:00Z | Yes             |
| Dynaimcs 365 F&O | inventtable  | IsDelete         | BOOLEAN   | No      | false                | No              |


