// Define main measures in f_salesinvoices and their budget comparators
var salesMeasuresWithComparators = new Dictionary<string, string>()
{
    { "Sales Invoiced", "Sales Budget" },
    { "Sales Margin", "Budget Margin" },
    { "Sales Volume", "Budget Volume" }
};

// Define the time periods for the comparisons (MTD, YTD) - "Today" removed
var timePeriods = new List<string>() { "MTD", "YTD" };

// Loop through each sales measure and corresponding budget measure
foreach (var kvp in salesMeasuresWithComparators)
{
    var salesMeasure = kvp.Key;
    var budgetMeasure = kvp.Value;

    // Loop through each time period (MTD, YTD)
    foreach (var period in timePeriods)
    {
        // Create the arrow logic expression with UNICHAR symbols
        var arrowExpression = 
            "IF(f_salesinvoices[" + salesMeasure + " (" + period + ")]" + " > " + 
            "f_budgets[" + budgetMeasure + " (" + period + ")]" + ", UNICHAR(8593), " +
            "IF(f_salesinvoices[" + salesMeasure + " (" + period + ")]" + " < " + 
            "f_budgets[" + budgetMeasure + " (" + period + ")]" + ", UNICHAR(8595), UNICHAR(45)))";

        // Add the arrow measure to f_salesinvoices table
        var newArrowMeasure = Model.Tables["f_salesinvoices"].AddMeasure(
            salesMeasure + " (" + period + " vs Budget) ARROW",  // Name
            arrowExpression,  // Arrow logic expression
            salesMeasure  // Display Folder
        );

        // Documentation for the arrow measure
        newArrowMeasure.Description = "This measure shows the arrow indicator for the " + salesMeasure + " vs " + budgetMeasure + " comparison for " + period + ".";

        // Create the text-based expression with Positive, Negative, No Change
        var textExpression = 
            "IF(f_salesinvoices[" + salesMeasure + " (" + period + ")]" + " > " + 
            "f_budgets[" + budgetMeasure + " (" + period + ")]" + ", \"Positive\", " +
            "IF(f_salesinvoices[" + salesMeasure + " (" + period + ")]" + " < " + 
            "f_budgets[" + budgetMeasure + " (" + period + ")]" + ", \"Negative\", \"No Change\"))";

        // Add the text comparison measure to f_salesinvoices table
        var newTextMeasure = Model.Tables["f_salesinvoices"].AddMeasure(
            salesMeasure + " (" + period + " vs Budget) TEXT",  // Name
            textExpression,  // Text logic expression
            salesMeasure  // Display Folder
        );

        // Documentation for the text measure
        newTextMeasure.Description = "This measure shows the text comparison (Positive, Negative, No Change) for the " + salesMeasure + " vs " + budgetMeasure + " for " + period + ".";
    }
}
