using System;
using System.IO;
using System.Text;
using System.Web;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.PersistentHashTable
{
    public class PersistentHashTable : IDisposable
    {
        private readonly Instance instance;
        private bool needToDisposeInstance;
        private readonly string database;
        private readonly string path;

        public Guid Id { get; private set; }

        public PersistentHashTable(string database)
        {
            this.database = database;
            path = database;
            if (Path.IsPathRooted(database) == false)
                path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, database);
            this.database = Path.Combine(path, Path.GetFileName(database));

            instance = new Instance(database + "_" + Guid.NewGuid());
        }

        public void Initialize()
        {
            instance.Parameters.CircularLog = true;
            instance.Parameters.CreatePathIfNotExist = true;
            instance.Parameters.TempDirectory = Path.Combine(path, "temp");
            instance.Parameters.SystemDirectory = Path.Combine(path, "system");
            instance.Parameters.LogFileDirectory = Path.Combine(path, "logs");

            try
            {
                instance.Init();

                needToDisposeInstance = true;

                EnsureDatabaseIsCreatedAndAttachToDatabase();

                SetIdFromDb();
            }
            catch (Exception e)
            {
                if (needToDisposeInstance)
                    instance.Dispose();
                needToDisposeInstance = false;
                throw new InvalidOperationException("Could not open cache: " + database, e);
            }
        }

        private void SetIdFromDb()
        {
            try
            {
                instance.WithDatabase(database, (session, dbid) =>
                {
                    using (var details = new Table(session, dbid, "details", OpenTableGrbit.ReadOnly))
                    {
                        Api.JetMove(session, details, JET_Move.First, MoveGrbit.None);
                        var columnids = Api.GetColumnDictionary(session, details);
                        var column = Api.RetrieveColumn(session, details, columnids["id"]);
                        Id = new Guid(column);
                        var schemaVersion = Api.RetrieveColumnAsString(session, details, columnids["schema_version"]);
                        if (schemaVersion != SchemaCreator.SchemaVersion)
                            throw new InvalidOperationException("The version on disk (" + schemaVersion + ") is different that the version supported by this library: " + SchemaCreator.SchemaVersion + Environment.NewLine +
                                                                "You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.");
                    }
                });
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Could not read db details from disk. It is likely that there is a version difference between the library and the db on the disk." + Environment.NewLine +
                "You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.", e);
            }
        }

        private void EnsureDatabaseIsCreatedAndAttachToDatabase()
        {
            using (var session = new Session(instance))
            {
                try
                {
                    Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
                    return;
                }
                catch (EsentErrorException e)
                {
                    if (e.Error != JET_err.FileNotFound)
                        throw;
                }

                new SchemaCreator(session).Create(database);
                Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
            }
        }

        public void Dispose()
        {
            if (needToDisposeInstance)
            {
                instance.Dispose();
            }
        }

        public void Batch(Action<PersistentHashTableActions> action)
        {
            using (var pht = new PersistentHashTableActions(instance, database, HttpRuntime.Cache, Id))
            {
                action(pht);
            }
        }
    }
}
