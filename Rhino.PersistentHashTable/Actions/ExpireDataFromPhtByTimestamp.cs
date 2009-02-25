using System;
using System.Text;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.PersistentHashTable.Actions
{
    public class ExpireDataFromPhtByTimestamp : EsentCommandBase
    {
        readonly DateTime lastDate;

        public ExpireDataFromPhtByTimestamp(string databasePath, DateTime lastDate)
            : base(databasePath)
        {
            this.lastDate = lastDate;
        }

        public void Excute()
        {
            ExecuteInDatabase((session, dbid) =>
            {
                using (var keys = new Table(session, dbid, "keys", OpenTableGrbit.None))
                using (var data = new Table(session, dbid, "data", OpenTableGrbit.None))
                {
                    Api.JetSetTableSequential(session, data, SetTableSequentialGrbit.None);
                    Api.JetSetCurrentIndex(session, keys, "pk");

                    var dataColumns = Api.GetColumnDictionary(session, data);
                    Api.MoveBeforeFirst(session, data);
                    while (Api.TryMoveNext(session, data))
                    {
                        var timeStampAsDouble = Api.RetrieveColumnAsDouble(session, data, dataColumns["timestamp"]);
                        var timestamp = DateTime.FromOADate(timeStampAsDouble.Value);
                        if (timestamp >= lastDate)
                            continue;

                        var key = Api.RetrieveColumnAsString(session, data, dataColumns["key"], Encoding.Unicode);
                        var version_instance_id = new Guid(
                            Api.RetrieveColumn(session, data, dataColumns["version_instance_id"])
                            );
                        var version_number =
                            Api.RetrieveColumnAsInt32(session, data, dataColumns["version_number"]).Value;

                        Api.JetDelete(session, data);

                        Api.MakeKey(session, keys, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
                        Api.MakeKey(session, keys, version_number, MakeKeyGrbit.None);
                        Api.MakeKey(session, keys, version_instance_id, MakeKeyGrbit.None);

                        if (Api.TrySeek(session, keys, SeekGrbit.SeekEQ))
                            Api.JetDelete(session, keys);
                    }
                }
            });
        }
    }
}