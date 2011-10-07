/* BadFile.cs
 * SSIS Script Loader by Karan Misra (kid0m4n)
 */

using System;
using System.Data;
using System.Linq;
using System.Collections.Generic;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;
using System.IO;
using System.Windows.Forms;
using System.Reflection;
using System.Xml.Linq;
using System.Data.OleDb;

[Microsoft.SqlServer.Dts.Pipeline.SSISScriptComponentEntryPointAttribute]
public class ScriptMain : UserComponent
{
    List<object[]> _rejectedRows;
    List<string> _listOfColumns;
    List<string> _listOfColumnsMod;

    class ColumnNamePair
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    List<ColumnNamePair> _columnMappings;

    public override void PreExecute()
    {
        base.PreExecute();

        _rejectedRows = new List<object[]>();
        _listOfColumns = new List<string>();

        for (int i = 0; i < ComponentMetaData.InputCollection[0].InputColumnCollection.Count; i++)
        {
            _listOfColumns.Add(ComponentMetaData.InputCollection[0].InputColumnCollection[i].Name);
        }

        _listOfColumnsMod = new List<string>(_listOfColumns.Select(x => x.Replace("_", "").Replace(" ", "")));

        var document = XDocument.Load(Variables.vPackagePath);

        XNamespace ns = "www.microsoft.com/SqlServer/Dts";

        var q = from x in document.Descendants(ns + "Property")
                let y = x.Attribute(ns + "Name")
                where y != null && y.Value == "ObjectName" && x.Value == Variables.TaskName
                select x.Parent;

        var dft = q.Single();

        var externalMetadataColumns = dft.Descendants("externalMetadataColumn").Where(x => x.Attribute("id") != null);
        var inputColumns = dft.Descendants("inputColumn").Where(x => x.Attributes("externalMetadataColumnId") != null);

        var mappings = from ic in inputColumns
                       join emc in externalMetadataColumns on ic.Attribute("externalMetadataColumnId").Value equals emc.Attribute("id").Value
                       select new { ColumnNo = Convert.ToInt32(ic.Attribute("id").Value), ColumnName = emc.Attribute("name").Value };

        _columnMappings = new List<ColumnNamePair>();

        foreach (var m in mappings)
        {
            _columnMappings.Add(new ColumnNamePair { Id = m.ColumnNo, Name = m.ColumnName });

        }
    }

    public string GetBadFileName(string tableName)
    {

        return string.Format("{0}_{1}.txt", tableName, DateTime.Now.ToString("yyyyMMdd"));
    }
    public void MB(object o)
    {
        MessageBox.Show("msg:" + o.ToString());
    }
    public override void PostExecute()
    {
        base.PostExecute();

        if (_rejectedRows.Count > 0)
        {
            try
            {
                using (var badFileWriter = new StreamWriter(Path.Combine(Variables.vBadFilesPath, GetBadFileName(Variables.vTableName))))
                {
                    _listOfColumns.Add("ErrorColumnName");
                    _listOfColumns.Add("ErrorDescription");

                    badFileWriter.WriteLine(_listOfColumns.Aggregate((x, y) => x + "," + y));

                    var errorColumnIndex = GetColumnIndex("ErrorColumn");
                    var errorCodeIndex = GetColumnIndex("ErrorCode");

                    foreach (var row in _rejectedRows)
                    {
                        var columnName = "-";
                        try
                        {
                            var matches = _columnMappings.Where(x => x.Id == (int)row[errorColumnIndex] && _listOfColumns.Contains(x.Name, StringComparer.CurrentCultureIgnoreCase));
                            columnName = matches.Single().Name;

                        }
                        catch
                        {

                            columnName = "-";
                        }
                        badFileWriter.Write(row.Select(x => x.ToString()).Aggregate((y, z) => y + "," + z));
                        badFileWriter.WriteLine(
                            string.Format(
                                ",{0},{1}",
                                columnName,
                                ComponentMetaData.GetErrorDescription((int)row[errorCodeIndex]).Trim()));
                    }
                }

                var conn = Connections.Staging.AcquireConnection(null);
                {
                    using (var cmd = new OleDbCommand("INSERT INTO dbo.SSIS_Failure_Log (Package_Name, Task_Name, Version, Error_Desc, Error_Time) VALUES (?, ?, ?, ?, GETDATE())", (OleDbConnection)conn))
                    {
                        cmd.Parameters.AddWithValue("@p1", Variables.PackageName);
                        cmd.Parameters.AddWithValue("@p2", Variables.TaskName);
                        cmd.Parameters.AddWithValue("@p3", string.Format("{0}.{1}.{2}", Variables.VersionMajor, Variables.VersionMinor, Variables.VersionBuild));
                        cmd.Parameters.AddWithValue("@p4", string.Format("Error in data detected. Bad file {0} created. Total errors = {1}", GetBadFileName(Variables.vTableName), _rejectedRows.Count));

                        cmd.ExecuteNonQuery();
                    }
                }

            }
            catch
            {
            }

            Variables.vRejectCount = _rejectedRows.Count;
        }
        else
        {
            var filePath = Path.Combine(Variables.vBadFilesPath, GetBadFileName(Variables.vTableName));
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

    public int GetColumnIndex(string col)
    {
        return _listOfColumns.FindIndex(x => x == col);
    }

    public object GetRowValue(PropertyInfo pi, Input0Buffer row)
    {
        var piCheckNull = (bool)typeof(Input0Buffer).GetProperty(pi.Name + "_IsNull").GetValue(row, null);

        if (piCheckNull)
            return "-";
        else
            return pi.GetValue(row, null);
    }

    public override void Input0_ProcessInputRow(Input0Buffer Row)
    {
        var badData = new List<object>(_listOfColumns.Count);
        typeof(Input0Buffer).GetProperties().Where(p => _listOfColumnsMod.Contains(p.Name)).ToList().ForEach(y => badData.Add(GetRowValue(y, Row)));

        _rejectedRows.Add(badData.ToArray());
    }
}