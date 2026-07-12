## Document Title: [UserLegalEntity Bridge Mapping]


## Details
The table is a bridge table that links users with legal entities. This table will be used for security roles. The user table will filter the bridge table which would filter the legal entity table and this will filter the fact tables in return.


## Silver Layer Tables

### User Role Organization
[ecoresproduct](./design/mapping/silverlayer/ecoresproduct.MD)

### Company
[ecoresproduct](./design/mapping/silverlayer/ecoresproduct.MD)


## Gold layer (UserLegalEntity Bridge Table)

### B_UserLegalEntity

#### Load Mode

- Incremental --> Watermark
- Full Load

| Stage | Source Table                    | Source Fields | Target Field             | Gold Field            | Data Type | IsNull? | DefaultValue | Partition Field | Semantic Model Field      | Type |
|-------|---------------------------------|---------------|--------------------------|--------------------------|-----------|---------|--------------|-----------------|------------------|------|
| 1     | cleansed_omuserroleorganization | recid         | UserLegalEntityNaturalID | UserLegalEntityNaturalID | STRING    | No      | 0            | No              |                  |      |
| 1     | cleansed_companyinfo            | dataarea      | LegalEntityNaturalID     |                          | STRING    | No      | Unknown      | No              |                  | ref  |
| 1     | cleansed_omuserroleorganization | user          | UserNaturalID            |                          | STRING    | No      | Unknown      | No              |                  | ref  |
| 1     | cleansed_omuserroleorganization | IsDelete      | IsDelete                 | IsDelete                 | BOOLEAN   | No      | False        | No              |                  |      |
| 2     | D_LegalEntity                   | LegalEntityID | FK_LegalEntityID         | FK_LegalEntityID         | STRING    | No      | -1           | No              | FK_LegalEntityID | fk   |
| 2     | D_User                          | UserID        | FK_UserID                | FK_UserID                | STRING    | No      | -1           | No              | FK_UserID        | fk   |



### Joins

| Join Stage | Join Type | Source Table                    | Join Table           | Source Table Keys      | Join Table Keys      |
|------------|-----------|---------------------------------|----------------------|------------------------|----------------------|
| 1          | LEFT      | cleansed_omuserroleorganization | cleansed_companyinfo | ominternalorganization | recid                |
| 2          | LEFT      | cleansed_omuserroleorganization | D_LegalEntity        | LegalEntityNaturalID   | LegalEntityNaturalID |
| 2          | LEFT      | cleansed_omuserroleorganization | D_User               | UserNaturalID          | UserNaturalID        |


