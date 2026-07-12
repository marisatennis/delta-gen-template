// Loop through all tables in the model
foreach (var table in Model.Tables)
{
    // Get the original table name
    string originalName = table.Name;

    // Check if the table name starts with "curated_"
    if (originalName.StartsWith("curated_"))
    {
        // Remove the "curated_" prefix
        string newName = originalName.Substring(8); // "curated_" is 8 characters long

        // Rename the table
        table.Name = newName;
    }
}
