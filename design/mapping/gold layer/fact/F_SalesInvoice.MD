## Document Title: [Sales Invoices Fact Mapping]

## Details
 The below maps the journey of the Sales Invoice Line stored in Dynamics 365 F&O from source to PowerBI. The purpose of this table is to record all the invoices at a product level. The invoice line would inherit customer and legal entity fields from the invoice level as they remain common for every invoice level.

## Silver Layer Tables

### Invoice Line
[inventtable](./design/mapping/silverlayer/ecoresproduct.MD)

### InvoiceHeader
[inventtable](./design/mapping/silverlayer/ecoresproduct.MD)

### Sales Line
[inventtable](./design/mapping/silverlayer/ecoresproduct.MD)


## Temp Tables

### Temp_tamrebateline
Describe what this temp table does.

| Source System    | Source Table         | Source Fields | Data Type | IsNull? | DefaultValue |
|------------------|----------------------|---------------|-----------|---------|--------------|
| Dynamics 365 F&O | silver_Tamrebateline | rebateid      | STRING    | No      | Unknown      |
| Dynamics 365 F&O | silver_Tamrebateline | rebateamount  | Float     | No      | Unknown      |
| Dynamics 365 F&O | silver_Tamrebateline | rebatelimit   | STRING    | No      | Unknown      |


### Temp Table Joins

| Join Type | From Table            | Join Table            | Source Table Keys | Join Table Keys |
|-----------|-----------------------|-----------------------|-------------------|-----------------|
| LEFT      | silver_tamrebateline  | silver_tamrebatetable | rebateid          | rebateid        |
| LEFT      | silver_tamrebatetable | silver_tamrebatedate  | rebateid          | rebateid        |

### Filters

| Table | Filter Field | Filter |
|-------|--------------|--------|

## Gold Layer (Sales Invoice Fact)

### F_SalesInvoiceLine

#### Load Mode

- Incremental --> Watermark = ModifiedOn
- Full Load


| ETL Stage       | Source Table              | Source Fields                                                                                                | Target Field              | Gold field                | Data Type | IsNull? | DefaultValue | Partition Field | Semantic Model Field | Type of Field |
|-----------------|---------------------------|--------------------------------------------------------------------------------------------------------------|---------------------------|---------------------------|-----------|---------|--------------|-----------------|----------------------|---------------|
| 1               | cleansed_custinvoicetrans | dataareaid + invoiceid + ISNULL(origsalesid,'NO-ORDERID') + linenum                                          | SalesInvoiceLineNaturalID | SalesInvoiceLineNaturalID | STRING    |         |              |                 |                      |               |
| 1               | cleansed_custinvoicetrans | invoiceid                                                                                                    | SalesInvoiceID            | SalesInvoiceID            | STRING    | No      | Unknown      | Yes             | Invoice ID           | ref           |
| 1               | cleansed_custinvoicetrans | salesunit                                                                                                    | UnitNaturalID             |                           | STRING    |         |              |                 |                      |               |
| 1               | cleansed_custinvoicetrans | itemid                                                                                                       | ProductNaturalID          |                           | STRING    |         |              |                 |                      |               |
| 1               | cleansed_custinvoicejour  | ISNULL(CONCAT(orderaccount,dataareaid),CONCAT(invoiceaccount,dataareaid))                                    | CustomerNaturalID         |                           | STRING    |         |              |                 |                      |               |
| 1               | cleansed_custinvoicejour  | dataareaid                                                                                                   | LegalEntityNaturalID      |                           | STRING    |         |              |                 |                      |               |
| 1               | cleansed_custinvoicejour  | invoicedate                                                                                                  | DateNaturalID             |                           | DATE      |         |              |                 |                      |               |
| 1               | cleansed_custinvoicejour  | invoicepostaladdress                                                                                         | InvoiceAddressNaturalID   |                           |           |         |              |                 |                      |               |
| 1               | cleansed_salesline        | deliverypostaladdress                                                                                        | PostalAddressNaturalID    |                           | BIGNINT   |         |              |                 |                      |               |
| 1               | D_Product                 | SupplierNaturalID                                                                                            | SupplierNaturalID         |                           | STRING    | Yes     | -1           | Yes             |                      |               |
| 1               | L_CustomerBUSalesTeam     | SalesTeamNaturalID                                                                                           | SalesTeamNaturalID        |                           | STRING    | Yes     | -1           | Yes             |                      |               |
| 1               |                           | CASE WHEN LegalEntityCode = "sbl" then D_Product.BusinessUnitNaturalID ELSE D_Customer.BusinessUnitNaturalID | BusinessUnitNaturalID     |                           | BIGINT    | No      | 0            | Yes             |                      |               |
| 1               | cleansed_custinvoicetrans | currencycode                                                                                                 | CurrencyNaturalID         |                           | STRING    | No      | Unknown      | Yes             |                      |               |
| 1               | cleansed_custinvoicetrans | qty                                                                                                          | Quantity                  | Quantity                  | INT       | No      | NULL         | Yes             | Quantity             | metric        |
| 1               | cleansed_custinvoicetrans | lineamount                                                                                                   | TotalAmount               |                           | FLOAT     | No      | NULL         | Yes             |                      |               |
| 1               | cleansed_custinvoicetrans | IsDelete                                                                                                     | IsDelete                  | IsDelete                  | BOOLEAN   | No      | false        | Yes             |                      |               |
| 1               | cleansed_salesline        | costprice                                                                                                    | CostPerUnit               |                           | FLOAT     | No      | NULL         | Yes             |                      |               |
| 1               | D_Product                 | NetWeight                                                                                                    | ProductWeight             |                           | FLOAT     | No      | NULL         | Yes             |                      |               |
| 1               | temp_tamrebateline        | RebateAmount                                                                                                 | RebateAmount              | RebateAmount              | FLOAT     | No      | NULL         | Yes             | RebateAmount         |               |
| 2               | D_Product                 | CASE WHEN ProductNaturalID IS NULL THEN 0 ELSE ISNULL(ProductID, -1)                                         | FK_ProductID              | FK_ProductID              | STRING    | No      | -1           | Yes             | FK_ProductID         | fk            |
| 2               | D_Unit                    | UnitID                                                                                                       | FK_UnitID                 | FK_UnitID                 | STRING    | No      | -1           | Yes             | FK_UnitID            | fk            |
| 2               | D_Customer                | CustomerID                                                                                                   | FK_CustomerID             | FK_CustomerID             | STRING    | No      | -1           | Yes             | FK_CustomerID        | fk            |
| 2               | D_LegalEntity             | LegalEntityID                                                                                                | FK_LegalEntityID          | FK_LegalEntityID          | STRING    | No      | -1           | Yes             | FK_LegalEntityID     | fk            |
| 2               | D_Date                    | DateID                                                                                                       | FK_DateID                 | FK_DateID                 | STRING    | No      | -1           | Yes             | FK_DateID            | fk            |
| 2               | D_BusinessUnit            | BusinessUnitID                                                                                               | FK_DivisionID             | FK_BusinessUnitID         | STRING    | No      | -1           | Yes             | FK_BusinessUnitID    | fk            |
| 2               | D_SalesTeam               | SalesTeamID                                                                                                  | FK_SalesTeamID            | FK_SalesTeamID            | STRING    | No      | -1           | Yes             | FK_SalesTeamID       | fk            |
| 2               | PostalAddress             | PostalAddressID                                                                                              | FK_DeliveryAddressID      | FK_DeliveryAddressID      | STRING    | No      | -1           | Yes             | FK_DeliveryAddressID | fk            |
| 2               | D_Supplier                | SupplierID                                                                                                   | FK_SupplierID             | FK_SupplierID             | STRING    | Yes     | -1           | Yes             | FK_SupplierID        | fk            |
| 2               | InvoiceAddress            | PostalAddressID                                                                                              | FK_InvoiceAddressID       | FK_InvoiceAddressID       | STRING    | No      | -1           | Yes             | FK_InvoiceAddressID  | fk            |
| 2               | D_BusinessUnit            | BusinessUnitID                                                                                               | FK_BusinessUnitID         | FK_BusinessUnitID         | STRING    | No      | -1           | Yes             | FK_BusinessUnitID    | fk            |
| transformation1 |                           | Quantity * ProductWeight                                                                                     | Volume                    | Volume                    | FLOAT     | No      | NULL         | Yes             | Volume               | metric        |
| transformation1 |                           | TotalAmount - (CostPerUnit * Quantity) - RebateAmount                                                        | TotalMargin               | TotalMargin               | FLOAT     | No      | NULL         | Yes             | Total                |               |
| Margin          | metric                    |                                                                                                              |                           |                           |           |         |              |                 |                      |               |
| 3               | EX_GBP                    | CASE WHEN CurrencyNaturalID = "GBP" THEN TotalAmount ELSE TotalAmount / EX_GBP.ExchangeRate                  | TotalAmount_GBP           | TotalAmount_GBP           | FLOAT     | No      | NULL         | Yes             | Total Amount (£)     | metric        |
| 3               | EX_GBP                    | CASE WHEN CurrencyNaturalID = "GBP" THEN TotalMargin ELSE TotalMargin / EX_GBP.ExchangeRate                  | TotalMargin_GBP           | TotalMargin_GBP           | FLOAT     | No      | NULL         | Yes             | Total Margin (£)     | metric        |
| 3               | EX_EUR                    | CASE WHEN CurrencyNaturalID = "EUR" THEN TotalAmount ELSE TotalAmount/ EX_EUR.ExchangeRate                   | TotalAmount_EUR           | TotalAmount_EUR           | FLOAT     | No      | NULL         | Yes             | Total Amount (€)     | metric        |
| 3               | EX_EUR                    | CASE WHEN CurrencyNaturalID = "EUR" THEN TotalMargin ELSE TotalMargin / EX_EUR.ExchangeRate                  | TotalMargin_EUR           | TotalMargin_EUR           | FLOAT     | No      | NULL         | Yes             | Total Margin (€)     | metric        |
| 3               | EX_USD                    | CASE WHEN CurrencyNaturalID = "USD" THEN TotalAmount ELSE TotalAmount / EX_USD.ExchangeRate                  | TotalAmount_USD           | TotalAmount_USD           | FLOAT     | No      | NULL         | Yes             | Total Amount ($)     | metric        |
| 3               | EX_USD                    | CASE WHEN CurrencyNaturalID = "USD" THEN TotalMargin ELSE TotalMargin / EX_USD.ExchangeRate                  | TotalMargin_USD           | TotalMargin_USD           | FLOAT     | No      | NULL         | Yes             | Total Margin ($)     | metric        |


#### Joins

Use UPPER In Joins

| ETL Stage | Join Type | From Table               | Join Table               | TableAlias     | Source Table Keys                                                         | Join Table Keys                          | Join Expression                                     |
|-----------|-----------|--------------------------|--------------------------|----------------|---------------------------------------------------------------------------|------------------------------------------|-----------------------------------------------------|
| 1         | LEFT      |                          | cleansed_custinvoicejour |                | invoiceid                                                                 | invoiceid                                |                                                     |
| 1         | LEFT      |                          | cleansed_salesline       |                | CONCAT(origsalesid,linenum,itemid)                                        | CONCAT(salesid,linenum,itemid)           |                                                     |
| 1         | LEFT      | cleanse_custinvoicetrans | cleansed_TAMrebatetable  |                | CONCAT(accountrelation,itemrelation,rebatecompanyid)                      | CONCAT(invoiceaccount,itemid,dataareaid) |                                                     |
| 1         | LEFT      | cleansed_TAMrebatetable  | cleansed_TAMrebateline   |                | Rebateid                                                                  | rebateid                                 |                                                     |
| 1         | LEFT      | cleansed_TAMrebatetable  | cleansed_TAMrebatedate   |                | Rebateid                                                                  | rebateid                                 | invoicedate BETWEEN rebatefromdate and rebatetodate |
| 1         | LEFT      |                          | D_Product                |                | itemid                                                                    | ProductNaturalID                         |                                                     |
| 1         | LEFT      | cleansed_custinvoicejour | D_Customer               |                | ISNULL(CONCAT(orderaccount,dataareaid),CONCAT(invoiceaccount,dataareaid)) | CustomerNaturalID                        |                                                     |
| 1         | LEFT      | cleansed_custinvoicejour | D_LegalEntity            |                | dataareaid                                                                | LegalEntityNaturalID                     |                                                     |
| 1         | LEFT      |                          | L_CustomerBUSalesTeam    |                | BusinessUnitNaturalID, CustomerNaturalID                                  | BusinessUnitNaturalID, CustomerNaturalID |                                                     |
| 2         | LEFT      |                          | D_Product                |                | ProductNaturalID                                                          | ProductNaturalID                         |                                                     |
| 2         | LEFT      |                          | D_Unit                   |                | UnitNaturalID                                                             | UnitNaturalID                            |                                                     |
| 2         | LEFT      |                          | D_Customer               |                | CustomerNaturalID                                                         | CustomerNaturalID                        |                                                     |
| 2         | LEFT      |                          | D_LegalEntity            |                | LegalEntityNaturalID                                                      | LegalEntityNaturalID                     |                                                     |
| 2         | LEFT      |                          | D_Date                   |                | DateNaturalID                                                             | DateNaturalID                            |                                                     |
| 2         | LEFT      |                          | D_PostalAddress          | PostalAddress  | PostalAddressNaturalID                                                    | PostalAddressNaturalID                   |                                                     |
| 2         | LEFT      |                          | D_PostalAddress          | InvoiceAddress | InvoiceAddressNaturalID                                                   | PostalAddressNaturalID                   |                                                     |
| 2         | LEFT      |                          | D_Supplier               |                | SupplierNaturalID                                                         | SupplierNaturalID                        |                                                     |
| 2         | LEFT      | L_CustomerBUSalesTeam    | D_SalesTeam              |                | SalesTeamNaturalID                                                        | SalesTeamNaturalID                       |                                                     |
| 2         | LEFT      | L_CustomerBUSalesTeam    | D_SalesTeam              |                | CONCAT(LegalEntityNaturalID,SalesTeamNaturalID)                           | SalesAreaNaturalID                       |                                                     |
| 2         | LEFT      |                          | D_BusinessUnit           |                | BusinessUnitNaturalID                                                     | BusinessUnitNaturalID                    |                                                     |
| 2         | LEFT      | Salesline                | D_SalesOrderStatus       |                | salesstatus                                                               | SalesOrderStatusNaturalID                |                                                     |
| 3         | LEFT      |                          | F_ExchangeRate           | EX_GBP         | FK_DateID                                                                 | FK_DateID                                |                                                     |
| 3         | LEFT      |                          | F_ExchangeRate           | EX_EUR         | FK_DateID                                                                 | FK_DateID                                |                                                     |
| 3         | LEFT      |                          | F_ExchangeRate           | EX_USD         | FK_DateID                                                                 | FK_DateID                                |                                                     |


### Filters

| ETL Stage | Table  | Filter Field      | Filter                                                                            |
|-----------|--------|-------------------|-----------------------------------------------------------------------------------|
| 3         | EX_GBP | FK_ToCurrencyID   | = (SELECT CurrencyID FROM D_Currency WHERE CurrencyNaturalID = CurrencyNaturalID) |
| 3         | EX_GBP | FK_FromCurrencyID | = (SELECT CurrencyID FROM D_Currency WHERE CurrencyNaturalID = "GBP")             |
| 3         | EX_EUR | FK_ToCurrencyID   | = (SELECT CurrencyID FROM D_Currency WHERE CurrencyNaturalID = CurrencyNaturalID) |
| 3         | EX_EUR | FK_FromCurrencyID | = (SELECT CurrencyID FROM D_Currency WHERE CurrencyNaturalID = "EUR")             |
| 3         | EX_USD | FK_ToCurrencyID   | = (SELECT CurrencyID FROM D_Currency WHERE CurrencyNaturalID = CurrencyNaturalID) |
| 3         | EX_USD | FK_FromCurrencyID | = (SELECT CurrencyID FROM D_Currency WHERE CurrencyNaturalID = "USD")             |


UPPER(d_salesorderstatus.SalesOrderStatus) = 'INVOICED'


#### Views

Filter IsDelete = false 


### PowerBI Measures

    No. Invoices =  
        SUMX(VALUES[F_SalesInvoice[Invoice Number]],1)
    
    Sum Total Amount = 
        var gbpamount = SUM(F_SalesInvoice[Total Amount (£)])
        var usdamount = SUM(F_SalesInvoice[Total Amount ($)])
        var euramount = SUM(F_SalesInvoice[Total Amount (€)])
        var currencyamount = SWITCH(
            SELECTEDVALUE(D_Currency[Currency Code]),
            "GBP", gbpamount,
            "USD", usdamount,
            "Euro", euroamount
        )
        return currencyamount
    
    Sum Total Margin = 
        var gbpmargin = SUM(F_SalesInvoice[Total Margin (£)])
        var usdmargin = SUM(F_SalesInvoice[Total Margin ($)])
        var eurmargin = SUM(F_SalesInvoice[Total Margin (€)])
        var currencymargin = SWITCH(
            SELECTEDVALUE(D_Currency[Currency Code])
            "GBP", gbpmargin,
            "USD", usdmargin,
            "EUR", euromargin
        )
        return currencymargin

    Sum Total Volume =
        SUM(F_SalesInvoice[Volume])

    Average Invoice Value = 
        AVERAGEX(F_SalesInvoice,F_SalesInvoice[Sum Total Amount])

    Average Invoice Margin =
        AVERAGEX(F_SalesInvoice[Sum Total Margin])

    Average Invoice Volume = 
        AVERAGEX(F_SalesInvoice[Sum Total Volume])

#### Derivatives
| Measure Name          | Derivative                  |
|-----------------------|-----------------------------|
| Sum Total Amount      | Today                       |
| Sum Total Amount      | Yesterday                   |
| Sum Total Amount      | Today vs Yesterday          |
| Sum Total Amount      | MTD                         |
| Sum Total Amount      | PMTD                        |
| Sum Total Amount      | MTD vs PMTD                 |
| Sum Total Amount      | YTD                         |
| Sum Total Amount      | PYTD                        |
| Sum Total Amount      | YTD vs PYTD                 |
| Sum Total Margin      | Today                       |
| Sum Total Margin      | Yesterday                   |
| Sum Total Margin      | MTD                         |
| Sum Total Margin      | PMTD                        |
| Sum Total Margin      | YTD                         |
| Sum Total Margin      | PYTD                        |
| Sum Total Margin      | YTD vs PYTD                 |
| Sum Total Volume      | Today                       |
| Sum Total Volume      | Yesterday                   |
| Sum Total Volume      | Today vs Yesterday          |
| Sum Total Volume      | MTD                         |
| Sum Total Volume      | PMTD                        |
| Sum Total Volume      | MTD vs PMTD                 |
| Sum Total Volume      | YTD                         |
| Sum Total Volume      | PYTD                        |
| Sum Total Volume      | YTD vs PYTD                 |
| Average Invoice Value | 12 Month Rolling Average    |
| Average Invoice Value | vs 12 Month Rolling Average |



#### FormatStrings

The currencies will use a dynamic format String to change the symbol based on the selected currency. The dynamic format String is a preview feature (at the time of development). In order to enable it File --> Options and Settings --> Options --> Preview Features --> Tick "Dynamic format String for measures"

In order to use the dynamic format String, click on the measure and then click on the drop down menu on the format String and select Dynamic. This will provide you with a an area where you can write DAX code to make your dynamic format Strings. Please note that once you create a dynamic format String for one measure, you can reference that format String for the other measure as well.

    Currency Format String
        var baseFormatString = "#,0.00"
        var currencySymbol = CALCULATE(D_Currency[Currency Symbol],SELECTEDVALUE([D_Currency[Currency Code]]))
        var formatString = currencySymbol & baseFormatString
        return formatString

The format String defined above will return the formatString based on the currency selected. If this format String was applied to the Total Invoice measure using the inSTRINGuctions above then you can reference this on other measures by doing the following:

    [_Total Invoice FormatString]

| Measure Name                                          | Format String                          |
|-------------------------------------------------------|----------------------------------------|
| Sum Total Amount                                      | Currency Format String (defined above) |
| Sum Total Amount Today                                | [_Sum Total Amount FormatString]       |
| Sum Total Amount Yesterday                            | [_Sum Total Amount FormatString]       |
| Sum Total Amount Today vs Yesterday                   | #0.0%#                                 |
| Sum Total Amount MTD                                  | [_Sum Total Amount FormatString]       |
| Sum Total Amount PMTD                                 | [_Sum Total Amount FormatString]       |
| Sum Total Amount MTD vs PMTD                          | #0.0%#                                 |
| Sum Total Amount YTD                                  | [_Sum Total Amount FormatString]       |
| Sum Total Amount PYTD                                 | [_Sum Total Amount FormatString]       |
| Sum Total Amount YTD vs PYTD                          | #0.0%#                                 |
| Sum Total Margin                                      | [_Sum Total Amount FormatString]       |
| Sum Total Margin Today                                | [_Sum Total Amount FormatString]       |
| Sum Total Margin Yesterday                            | [_Sum Total Amount FormatString]       |
| Sum Total Margin Today vs Yesterday                   | #0.0%#                                 |
| Sum Total Margin MTD                                  | [_Sum Total Amount FormatString]       |
| Sum Total Margin PMTD                                 | [_Sum Total Amount FormatString]       |
| Sum Total Margin MTD vs PMTD                          | #0.0%#                                 |
| Sum Total Margin YTD                                  | [_Sum Total Amount FormatString]       |
| Sum Total Margin PYTD                                 | [_Sum Total Amount FormatString]       |
| Sum Total Margin YTD vs PYTD                          | #0.0%#                                 |
| Average Invoice Value                                 | [_Sum Total Amount FormatString]       |
| Average Invoice Value 12 Month Rolling Average        | [_Sum Total Amount FormatString]       |
| Average Invoice Margin vs vs 12 Month Rolling Average | #0.0%#                                 |
| Average Invoice Margin                                | [_Sum Total Amount FormatString]       |
| Sum Total Volume                                      | ##0.0##                                |
| Sum Total Volume Today                                | ##0.0##                                |
| Sum Total Volume Yesterday                            | ##0.0##                                |
| Sum Total Volume Today vs Yesterday                   | ##0.0%##                               |
| Sum Total Volume MTD                                  | ##0.0##                                |
| Sum Total Volume PMTD                                 | ##0.0##                                |
| Sum Total Volume MTD vs PMTD                          | ##0.0%##                               |
| Sum Total Volume YTD                                  | ##0.0##                                |
| Sum Total Volume PYTD                                 | ##0.0##                                |
| Sum Total Volume YTD vs PYTD                          | ##0.0%##                               |
| Average Invoice Volume                                | ##0.0##                                |


