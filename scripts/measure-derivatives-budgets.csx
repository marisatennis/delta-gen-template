// Define base measures for budgets and their derivatives
var budgetDerivatives = new Dictionary<string, List<string>>()
{
    { "Sales Budget", new List<string>() { "MTD", "YTD" } },
    { "Budget Margin", new List<string>() { "MTD", "YTD" } },
    { "Budget Volume", new List<string>() { "MTD", "YTD" } }
};

// Define derivative expressions
Dictionary<string, string> BudgetDerivativeExpressions = new Dictionary<string, string>()
{
    { "MTD", "VAR CurrentDate = TODAY() VAR StartdateMonth = DATE(YEAR(CurrentDate), MONTH(CurrentDate), 1) RETURN CALCULATE({0},'d_date'[Date] >= StartdateMonth && 'd_date'[Date] <= CurrentDate, ALLSELECTED('d_date'[Year], 'd_date'[Financial Year]))" },
    { "YTD", "{0}" }
};

foreach (var kvp in budgetDerivatives)
{
    var budgetMeasureName = kvp.Key;
    var budgetDerivativesList = kvp.Value;

    foreach (var derivative in budgetDerivativesList)
    {
        if (BudgetDerivativeExpressions.ContainsKey(derivative))
        {
            var budgetMeasureExpression = string.Format(BudgetDerivativeExpressions[derivative], "[" + budgetMeasureName + "]");

            // Create the budget derivative measure in f_budgets table
            var newBudgetMeasure = Model.Tables["f_budgets"].AddMeasure(
                budgetMeasureName + " (" + derivative + ")",  // Name
                budgetMeasureExpression,  // Expression
                budgetMeasureName  // Display Folder
            );

            // Set format string for measures
            newBudgetMeasure.FormatString = "Currency"; // Adjust if needed

            // Documentation
            newBudgetMeasure.Description = "This is a derivative of " + budgetMeasureName + ".";
        }
    }
}
