using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.PersistentHashTable.Actions
{
    public class ExportToCsv : EsentCommandBase
    {
        const char quote = '"';
        const string comma = ",";
        private readonly string pathToExportTo;

        public ExportToCsv(string databasePath, string pathToExportTo) : base(databasePath)
        {
            this.pathToExportTo = pathToExportTo;
        }

        public void Execute()
        {
            ExecuteInDatabase((session,dbid) =>
            {
                DumpTable(session, dbid, "keys");
                DumpTable(session, dbid, "data");
                DumpTable(session, dbid, "details");
                DumpTable(session, dbid, "lists");
            });
        }

        private void DumpTable(Session session, JET_DBID dbid, string tableName)
        {
            using(TextWriter writer = File.CreateText(Path.Combine(pathToExportTo, tableName+".csv")))

            {
                var columnFormatters = new List<Func<JET_SESID, JET_TABLEID, string>>();
                var columnNames = new List<string>();

                foreach (ColumnInfo column in Api.GetTableColumns(session, dbid, tableName))
                {
                    columnNames.Add(column.Name);

                    // create a local variable that will be captured by the lambda functions below
                    var columnid = column.Columnid;
                    switch (column.Coltyp)
                    {
                        case JET_coltyp.Bit:
                            columnFormatters.Add(
                                (s, t) => String.Format("{0}", Api.RetrieveColumnAsBoolean(s, t, columnid)));
                            break;
                        case JET_coltyp.Currency:
                            columnFormatters.Add(
                                (s, t) => String.Format("{0}", Api.RetrieveColumnAsInt64(s, t, columnid)));
                            break;
                        case JET_coltyp.IEEEDouble:
                            columnFormatters.Add(
                                (s, t) => String.Format("{0}", Api.RetrieveColumnAsDouble(s, t, columnid)));
                            break;
                        case JET_coltyp.IEEESingle:
                            columnFormatters.Add(
                                (s, t) => String.Format("{0}", Api.RetrieveColumnAsFloat(s, t, columnid)));
                            break;
                        case JET_coltyp.Long:
                            columnFormatters.Add(
                                (s, t) => String.Format("{0}", Api.RetrieveColumnAsInt32(s, t, columnid)));
                            break;
                        case JET_coltyp.Text:
                        case JET_coltyp.LongText:
                            var encoding = (column.Cp == JET_CP.Unicode) ? Encoding.Unicode : Encoding.ASCII;
                            columnFormatters.Add(
                                (s, t) => String.Format("{0}", Api.RetrieveColumnAsString(s, t, columnid, encoding)));
                            break;
                        case JET_coltyp.Short:
                            columnFormatters.Add(
                                (s, t) => String.Format("{0}", Api.RetrieveColumnAsInt16(s, t, columnid)));
                            break;
                        case JET_coltyp.UnsignedByte:
                            columnFormatters.Add(
                                (s, t) => String.Format("{0}", Api.RetrieveColumnAsByte(s, t, columnid)));
                            break;
                        case JET_coltyp.Binary:
                            if((column.Grbit & ColumndefGrbit.ColumnMultiValued) == ColumndefGrbit.ColumnMultiValued)
                            {
                                columnFormatters.Add((s,t)=>
                                {
                                    var sb = new StringBuilder();
                                    int index = 1;
                                    int size = -1;
                                    while (size != 0)
                                    {
                                        var buffer = new byte[20];
                                        Api.JetRetrieveColumn(s, t, columnid, buffer, 20, out size,
                                                              RetrieveColumnGrbit.None, new JET_RETINFO
                                                              {
                                                                  itagSequence = index
                                                              });
                                        if (size == 0)
                                            break;
                                        index += 1;
                                        var guidBuffer = new byte[16];
                                        Buffer.BlockCopy(buffer, 0, guidBuffer, 0, 16);
                                        if (sb.Length > 0)
                                            sb.AppendLine();
                                        sb.Append(new Guid(guidBuffer))
                                            .Append("/")
                                            .Append(BitConverter.ToInt32(buffer, 16));
                                    }
                                    return sb.ToString();
                                });
                            }
                            else if(column.MaxLength==16)//guid!
                                columnFormatters.Add((s,t) => new Guid(Api.RetrieveColumn(s,t,columnid)).ToString());
                            else
                            {
                                columnFormatters.Add((s, t) =>
                                {
                                    var array = Api.RetrieveColumn(s, t, columnid);
                                    if (array == null)
                                    {
                                        return "NULL";
                                    }
                                    return Convert.ToBase64String(array);
                                });}
                            break;
                            //case JET_coltyp.LongBinary:
                        case JET_coltyp.DateTime:
                            columnFormatters.Add((s, t) =>
                            {
                                var d = Api.RetrieveColumnAsDouble(s, t, columnid);
                                if (d == null)
                                    return "NULL";
                                return DateTime.FromOADate(d.Value).ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
                            });
                            break;
                        default:
                            columnFormatters.Add((s, t) =>
                            {
                                var array = Api.RetrieveColumn(s, t, columnid);
                                if (array == null)
                                    return "NULL";
                                return Convert.ToBase64String(array);
                            });
                            break;
                    }
                }

                writer.WriteLine(String.Join(comma, columnNames.ToArray()));

                using (Table table = new Table(session, dbid, tableName, OpenTableGrbit.ReadOnly))
                {
                    Api.JetSetTableSequential(session, table, SetTableSequentialGrbit.None);

                    Api.MoveBeforeFirst(session, table);
                    while (Api.TryMoveNext(session, table))
                    {
                        var columnData = from formatter in columnFormatters
                                         select QuoteForCsv(formatter(session, table));
                        writer.WriteLine(String.Join(comma, columnData.ToArray()));
                    }

                    Api.JetResetTableSequential(session, table, ResetTableSequentialGrbit.None);
                }

                writer.Flush();
            }
        }

        internal static string QuoteForCsv(string s)
        {
            if (String.IsNullOrEmpty(s))
            {
                return s;
            }


            // first, double any existing quotes
            if (s.Contains(quote))
            {
                s = s.Replace("\"", "\"\"");
            }

            // check to see if we need to add quotes
            // there are five cases where this is needed:
            //  1. Value starts with whitespace
            //  2. Value ends with whitespace
            //  3. Value contains a comma
            //  4. Value contains a quote
            //  5. Value contains a newline
            if (Char.IsWhiteSpace(s[0])
                || Char.IsWhiteSpace(s[s.Length - 1])
                || s.Contains(comma)
                || s.Contains(quote)
                || s.Contains(Environment.NewLine))
            {
                s = String.Format("\"{0}\"", s);
            }

            return s;
        }
    }
}