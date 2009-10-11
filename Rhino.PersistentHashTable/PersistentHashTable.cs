using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.ConstrainedExecution;
using System.Text;
using System.Threading;
using System.Web;
using Microsoft.Isam.Esent.Interop;
using System.Diagnostics;

namespace Rhino.PersistentHashTable
{
	public class PersistentHashTable : CriticalFinalizerObject, IDisposable
	{
		private JET_INSTANCE instance;
		private readonly string database;
		private readonly string path;
		private IVersionGenerator versionGenerator;
		private IDictionary<string, JET_COLUMNID> keysColumns;
		private IDictionary<string, JET_COLUMNID> dataColumns;
		private IDictionary<string, JET_COLUMNID> listColumns;
		private IDictionary<string, JET_COLUMNID> replicationColumns;
		private IDictionary<string, JET_COLUMNID> replicationRemovalColumns;

		public Guid Id { get; private set; }

		public PersistentHashTable(string database)
		{
			this.database = database;
			path = database;
			if (Path.IsPathRooted(database) == false)
				path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, database);
			this.database = Path.Combine(path, Path.GetFileName(database));
			Api.JetCreateInstance(out instance, database + Guid.NewGuid());

		}

		public void Initialize()
		{
			ConfigureInstance(instance);
			try
			{
				Api.JetInit(ref instance);

				EnsureDatabaseIsCreatedAndAttachToDatabase();

				SetIdFromDb();

				GatherColumnsIds();

				versionGenerator = new HiLoVersionGenerator(instance, 4096, database);
			}
			catch (Exception e)
			{
				Dispose();
				throw new InvalidOperationException("Could not open cache: " + database, e);
			}
		}

		private void GatherColumnsIds()
		{
			using (var session = new Session(instance))
			{
				JET_DBID dbid;
				Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);
				using (var keys = new Table(session, dbid, "keys", OpenTableGrbit.None))
					keysColumns = Api.GetColumnDictionary(session, keys);
				using (var data = new Table(session, dbid, "data", OpenTableGrbit.None))
					dataColumns = Api.GetColumnDictionary(session, data);
				using (var list = new Table(session, dbid, "lists", OpenTableGrbit.None))
					listColumns = Api.GetColumnDictionary(session, list);
				using (var replication = new Table(session, dbid, "replication_info", OpenTableGrbit.None))
					replicationColumns = Api.GetColumnDictionary(session, replication);
				using (var replicationRemoval = new Table(session, dbid, "replication_removal_info", OpenTableGrbit.None))

					replicationRemovalColumns = Api.GetColumnDictionary(session, replicationRemoval);

				Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);
			}
		}

		private void ConfigureInstance(JET_INSTANCE jetInstance)
		{
			var parameters = new InstanceParameters(jetInstance)
			{
				CircularLog = true,
				Recovery = true,
				CreatePathIfNotExist = true,
				TempDirectory = Path.Combine(path, "temp"),
				SystemDirectory = Path.Combine(path, "system"),
				LogFileDirectory = Path.Combine(path, "logs"),
				MaxVerPages = 8192,
				MaxTemporaryTables = 8192,
			};
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
					if (e.Error == JET_err.DatabaseDirtyShutdown)
					{
						try
						{
							using (var recoverInstance = new Instance("Recovery instance for: " + database))
							{
								recoverInstance.Init();
								using (var recoverSession = new Session(recoverInstance))
								{
									ConfigureInstance(recoverInstance.JetInstance);
									Api.JetAttachDatabase(recoverSession, database,
														  AttachDatabaseGrbit.DeleteCorruptIndexes);
									Api.JetDetachDatabase(recoverSession, database);
								}
							}
						}
						catch (Exception)
						{
						}

						Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
						return;
					}
					if (e.Error != JET_err.FileNotFound)
						throw;
				}

				new SchemaCreator(session).Create(database);
				Api.JetAttachDatabase(session, database, AttachDatabaseGrbit.None);
			}
		}

		public void Dispose()
		{
			GC.SuppressFinalize(this);
			Api.JetTerm2(instance, TermGrbit.Abrupt);
		}

		~PersistentHashTable()
		{
			try
			{
				Trace.WriteLine(
					"Disposing esent resources from finalizer! You should call PersistentHashTable.Dispose() instead!");
				Api.JetTerm(instance);
			}
			catch (Exception exception)
			{
				try
				{
					Trace.WriteLine("Failed to dispose esent instance from finalizer because: " + exception);
				}
				catch
				{
				}
			}
		}

		[CLSCompliant(false)]
		public void Batch(Action<PersistentHashTableActions> action)
		{
			if (versionGenerator == null)
				throw new InvalidOperationException("The PHT was not initialized. Did you forgot to call table.Initialize(); ?");

			for (int i = 0; i < 5; i++)
			{
				try
				{
					using (var pht = new PersistentHashTableActions(
						instance, database, HttpRuntime.Cache, Id, versionGenerator,
						keysColumns, listColumns, replicationColumns, replicationRemovalColumns, dataColumns))
					{
						action(pht);
					}
					return;
				}
				// if we run into a write conflict, we will wait a bit and then retry
				catch (EsentErrorException e)
				{
					if (e.Error != JET_err.WriteConflict)
						throw;
					Thread.Sleep(10);
				}
			}
		}
	}
}
