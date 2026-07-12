// Loop through all tables in the model
foreach (var table in Model.Tables)
{
    // Loop through all columns in the current table
    foreach (var column in table.Columns.ToList())
    {
        // Get the original column name
        string originalName = column.Name;

        // Check if the column name contains "ID" or "Delete"
        if (!originalName.Contains("ID") && !originalName.Contains("Delete"))
        {
            // Insert spaces before capital letters, except for the first character
            string newName = "";
            foreach (char c in originalName)
            {
                if (char.IsUpper(c) && newName.Length > 0)
                    newName += " " + c;
                else
                    newName += c;
            }
            
            // Trim and rename the column
            newName = newName.Trim();
            if (originalName != newName)
            {
                column.Name = newName;
            }
        }
    }
}
