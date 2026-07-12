// Define main measures and their derivatives
var measureDerivatives = new Dictionary<string, List<string>>()
{
    { "Sum COMAH Lower Tier Score", new List<string>() { "Today", "Yesterday", "Today vs Yesterday" } },
    { "Sum COMAH Upper Tier Score", new List<string>() { "Today", "Yesterday", "Today vs Yesterday" } },
};

Dictionary<string, string> derivativeExpressions = new Dictionary<string, string>()
{
    { "Today", "VAR CurrentDate = TODAY() RETURN CALCULATE({0},'d_date'[Date] = CurrentDate, ALLSELECTED('d_date'[Year], 'd_date'[Financial Year]))" },
    { "Yesterday", "VAR YesterdayDate = TODAY() – 1 RETURN CALCULATE({0},'d_date'[Date] = YesterdayDate, ALLSELECTED('d_date'[Year], 'd_date'[Financial Year]))" }
    // Add expressions for other derivatives as needed
};

foreach(var kvp in measureDerivatives)
{
    var mainMeasureName = kvp.Key;
    var derivatives = kvp.Value;

    foreach (var derivativeName in derivatives)
    {
        Console.WriteLine("Derivative Name: " + derivativeName); // Debugging statement

        if (derivativeExpressions.ContainsKey(derivativeName))
        {
            var measureExpression = string.Format(derivativeExpressions[derivativeName], "[" + mainMeasureName + "]");

            var newMeasure = Model.Tables["F_ComahSubstanceSnapshot"].AddMeasure(
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
                var newArrowMeasure = Model.Tables["F_ComahSubstanceSnapshot"].AddMeasure(
                mainMeasureName + " (" + derivativeName + ") ARROW",  // Name
                "IF([" + mainMeasureName + " (" + derivativeName + ")] > 0, UNICHAR(8593), IF[" + mainMeasureName + " (" + derivativeName + ")] = 0, UNICHAR(45), UNICHAR(8595))) ", //Expression
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
    }
};

foreach (var kvp in measureDerivatives)
{
    var mainMeasureName = kvp.Key;
    var derivatives = kvp.Value;

    foreach (var comparatorName in ComparatorExpressions.Keys)
    {
        if (ComparatorExpressions.ContainsKey(comparatorName))
        {
            // Generate DAX for the comparator measure
            var comparatorExpression = string.Format(ComparatorExpressions[comparatorName], mainMeasureName);

            var newComparatorMeasure = Model.Tables["F_ComahSubstance"].AddMeasure(
                mainMeasureName + " (" + comparatorName + ")",  // Name
                comparatorExpression, // Expression
                mainMeasureName // Display Folder
            );

            newComparatorMeasure.FormatString = "#0.0%#"; // Assuming all comparator measures are percentages

             // Create a new arrow measure for the comparator
            var newArrowMeasure = Model.Tables["F_ComahSubstance"].AddMeasure(
                mainMeasureName + " (" + comparatorName + ") ARROW",  // Name
                "IF([" + mainMeasureName + " (" + comparatorName + ")] > 0, UNICHAR(8593), IF([" + mainMeasureName + " (" + comparatorName + ")] = 0, UNICHAR(45), UNICHAR(8595))) ",  // Arrow logic
                mainMeasureName // Display Folder
            );

            // Provide some documentation for the arrow measure
            newArrowMeasure.Description = "This measure is a derivative of " + mainMeasureName + ".";
        }
        else
        {
            Console.WriteLine("Comparator Expression not found for: " + comparatorName);
        }
    }
}