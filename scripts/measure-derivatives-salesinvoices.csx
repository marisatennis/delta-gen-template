// Define main measures and their derivatives
var measureDerivatives = new Dictionary<string, List<string>>()
{
    { "Sales Invoiced", new List<string>() { "Today", "Yesterday", "Today vs Yesterday", "MTD", "PMTD", "MTD vs PMTD", "YTD", "PYTD", "YTD vs PYTD", "Last Month"} },
    { "Sales Margin", new List<string>() { "Today", "Yesterday", "Today vs Yesterday", "MTD", "PMTD", "MTD vs PMTD", "YTD", "PYTD", "YTD vs PYTD" } },
    { "Sales Volume", new List<string>() { "Today", "MTD", "YTD", "PYTD"} },
    { "No. Invoices", new List<string>() { "Today", "MTD"  } },
    { "Total Cost", new List<string>() { "Today" , "MTD" }},
    { "Margin %", new List<string>() { "Today" , "MTD" }}
};

Dictionary<string, string> DerivativeExpressions = new Dictionary<string, string>()
{
    { "Today", "VAR CurrentDate = TODAY() RETURN CALCULATE({0},'d_date'[Date] = CurrentDate, ALLSELECTED('d_date'[Year], 'd_date'[Financial Year]))" },
    { "Yesterday", "VAR YesterdayDate = TODAY() – 1 RETURN CALCULATE({0},'d_date'[Date] = YesterdayDate, ALLSELECTED('d_date'[Year], 'd_date'[Financial Year]))" },
    { "MTD", "VAR CurrentDate = TODAY() VAR StartdateMonth = DATE(YEAR(CurrentDate), MONTH(CurrentDate), 1) RETURN CALCULATE({0},'d_date'[Date] >= StartdateMonth && 'd_date'[Date] <= CurrentDate, ALLSELECTED('d_date'[Year], 'd_date'[Financial Year]))" },
    { "PMTD", "VAR CurrentDate = TODAY() VAR DaysInMonthSoFar = DAY(CurrentDate) VAR StartOfPriorMonth = EDATE(DATE(YEAR(CurrentDate), MONTH(CurrentDate), 1), -1) VAR EndOfPriorMonth = StartOfPriorMonth + DaysInMonthSoFar – 1 RETURN CALCULATE({0},'d_date'[Date] >= StartOfPriorMonth && 'd_date'[Date] <= EndOfPriorMonth, ALLSELECTED('d_date'[Year], 'd_date'[Financial Year]))" },
    { "YTD", "{0}" },
    { "PYTD", "CALCULATE ({0}, REMOVEFILTERS ( d_date ), FILTER (DATEADD ( d_date[Date], -1, YEAR ), d_date[Date] <= DATE ( YEAR ( TODAY () - 1 ), MONTH ( TODAY () ), DAY ( TODAY () ) )))" },
    { "Last Month", "VAR CurrentDate = TODAY() VAR StartOfLastMonth = EOMONTH(CurrentDate, -2) + 1 VAR EndOfLastMonth = EOMONTH(CurrentDate, -1) RETURN CALCULATE({0}, 'd_date'[Date] >= StartOfLastMonth && 'd_date'[Date] <= EndOfLastMonth, ALLSELECTED('d_date'[Year], 'd_date'[Financial Year]))" }
   

    // Add expressions for other derivatives as needed
};

foreach(var kvp in measureDerivatives)
{
    var mainMeasureName = kvp.Key;
    var derivatives = kvp.Value;

    foreach (var derivativeName in derivatives)
    {
        Console.WriteLine("Derivative Name: " + derivativeName); // Debugging statement

        if (DerivativeExpressions.ContainsKey(derivativeName))
        {
            var measureExpression = string.Format(DerivativeExpressions[derivativeName], "[" + mainMeasureName + "]");

            var newMeasure = Model.Tables["f_salesinvoices"].AddMeasure(
                mainMeasureName + " (" + derivativeName + ")",  // Name
                measureExpression, //Expression
                mainMeasureName                            // Display Folder
            );
            

            // Provide some documentation:
            newMeasure.Description = "This measure is a derivative of " + mainMeasureName + ".";

              // Set the format string on the new measure:
            if (newMeasure.Name.Contains("vs"))
            {
                newMeasure.FormatString = "#0.0%#";
                var newArrowMeasure = Model.Tables["f_salesinvoices"].AddMeasure(
                mainMeasureName + " (" + derivativeName + ") ARROW",  // Name
                "IF([" + mainMeasureName + " (" + derivativeName + ")] > 0, UNICHAR(8593), IF[" + mainMeasureName + " (" + derivativeName + ")] = 0, UNICHAR(45), UNICHAR(8595)))  ", //Expression
                mainMeasureName                            // Display Folder
                );
                // Provide some documentation:
                newArrowMeasure.Description = "This measure is a derivative of " + mainMeasureName + ".";
                
            }

            else if (newMeasure.Name.Contains("No."))
            {newMeasure.FormatString = "#0";}

            else
            {
                newMeasure.FormatStringExpression = "[Currency Format String]";
            }
        }
        else
        {
            Console.WriteLine("Derivative Expression not found for: " + derivativeName); // Debugging statement
        }
    }
}



// Define the comparator expressions
Dictionary<string, string> ComparatorExpressions = new Dictionary<string, string>()
{
    { "Today vs Yesterday", 
        "IF (ISBLANK([{0} (Yesterday)]), BLANK(), DIVIDE ([{0} (Today)] - [{0} (Yesterday)], [{0} (Yesterday)], 0))" 
    },
    { "MTD vs PMTD", 
        "IF (ISBLANK([{0} (PMTD)]), BLANK(), DIVIDE ([{0} (MTD)] - [{0} (PMTD)], [{0} (PMTD)], 0))" 
    },
    { "YTD vs PYTD", 
        "IF (ISBLANK([{0} (PYTD)]), BLANK(), DIVIDE ([{0} (YTD)] - [{0} (PYTD)], [{0} (PYTD)], 0))" 
    }
};

// Specify which measures should have comparator derivatives
List<string> measuresWithComparators = new List<string>() { "Sales Invoiced", "Sales Margin" };

// Loop through the measures
foreach (var kvp in measureDerivatives)
{
    var mainMeasureName = kvp.Key;
    
    // Only process comparator derivatives for the specific measures
    if (measuresWithComparators.Contains(mainMeasureName))
    {
        foreach (var comparatorName in ComparatorExpressions.Keys)
        {
            if (ComparatorExpressions.ContainsKey(comparatorName))
            {
                // Generate DAX for the comparator measure
                var comparatorExpression = string.Format(ComparatorExpressions[comparatorName], mainMeasureName);

                var newComparatorMeasure = Model.Tables["f_salesinvoices"].AddMeasure(
                    mainMeasureName + " (" + comparatorName + ")",  // Name
                    comparatorExpression, // Expression
                    mainMeasureName // Display Folder
                );

                // Format as percentage
                newComparatorMeasure.FormatString = "#0.0%#";

                   // Set the format string on the new measure:
                var newArrowMeasure = Model.Tables["f_salesinvoices"].AddMeasure(
                    mainMeasureName + " (" + comparatorName + ") ARROW",  // Name
                    "IF([" + mainMeasureName + " (" + comparatorName + ")] > 0, UNICHAR(8593), IF([" + mainMeasureName + " (" + comparatorName + ")] = 0, UNICHAR(45), UNICHAR(8595))) " ,  // Arrow logic
                    mainMeasureName // Display Folder
                );

                // Provide some documentation:
                newArrowMeasure.Description = "This measure is a derivative of " + mainMeasureName + ".";
            }
            else
            {
                Console.WriteLine("Comparator Expression not found for: " + comparatorName);
            }
            
        }
    }
}