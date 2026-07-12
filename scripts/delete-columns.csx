// Loop through all tables in the model
foreach (var table in Model.Tables)
{
    // Collect columns to delete
    var columnsToDelete = new List<Column>();

    // Loop through all columns in the current table
    foreach (var column in table.Columns.ToList()) // Use ToList() to avoid collection modification issues
    {
        // Get the column name
        string columnName = column.Name;

        // Check if the column name contains "NaturalID" or is exactly "IsDelete"
        if (columnName.Contains("NaturalID") || columnName == "IsDelete")
        {
            // Add column to the deletion list
            columnsToDelete.Add(column);
        }
    }

    // Delete the collected columns
    foreach (var column in columnsToDelete)
    {
        column.Delete();
    }
}
