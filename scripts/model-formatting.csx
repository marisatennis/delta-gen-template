foreach( var m in Model.AllMeasures)
{
    m.FormatDax();
}

foreach (var c in Model.AllColumns)
{
    c.IsHidden = c.Name.Contains("FK");
}

foreach( var t in Model.Tables)
{
    t.IsHidden = t.Name.Contains("b_");
}