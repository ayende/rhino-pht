using System;
using System.IO;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.PersistentHashTable.Actions
{
    public class EsentCommandBase
    {
        protected string databasePath;

        public EsentCommandBase(string databasePath)
        {
            this.databasePath = databasePath;
        }

        protected void ExecuteInDatabase(Action<Session, JET_DBID> action)
        {
            var instance = new Instance(databasePath);
            try
            {
                instance.Parameters.CircularLog = true;
                instance.Parameters.CreatePathIfNotExist = true;
                instance.Parameters.TempDirectory = Path.Combine(Path.GetDirectoryName(databasePath), "temp");
                instance.Parameters.SystemDirectory = Path.Combine(Path.GetDirectoryName(databasePath), "system");
                instance.Parameters.LogFileDirectory = Path.Combine(Path.GetDirectoryName(databasePath), "logs");
                instance.Init();

                using (var session = new Session(instance))
                {
                    Api.JetAttachDatabase(session, databasePath, AttachDatabaseGrbit.None);
                    try
                    {
                        using (var tx = new Transaction(session))
                        {

                            JET_DBID dbid;
                            Api.JetOpenDatabase(session, databasePath, "", out dbid, OpenDatabaseGrbit.None);
                            try
                            {
                                action(session, dbid);
                                tx.Commit(CommitTransactionGrbit.None);
                            }
                            finally
                            {  
                                Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
                            }
                        }
                    }
                    finally
                    {
                        Api.JetDetachDatabase(session, databasePath);
                    }
                }
            }
            finally
            {
                instance.Term();
            }
        }
    }
}