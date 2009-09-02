using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web.Caching;
using Microsoft.Isam.Esent.Interop;

namespace Rhino.PersistentHashTable
{
	[CLSCompliant(false)]
	public class PersistentHashTableActions : IDisposable
	{
		private readonly Cache cache;
		private readonly List<Action> commitSyncronization = new List<Action>();
		private readonly Table data;
		private readonly IDictionary<string, JET_COLUMNID> dataColumns;
		private readonly JET_DBID dbid;
		private readonly Table identity;
		private readonly IDictionary<string, JET_COLUMNID> identityColumns;
		private readonly Guid instanceId;
		private readonly Table keys;
		private readonly IDictionary<string, JET_COLUMNID> keysColumns;
		private readonly Table list;
		private readonly IDictionary<string, JET_COLUMNID> listColumns;
		private readonly Table replication;
		private readonly IDictionary<string, JET_COLUMNID> replicationColumns;
		private readonly Table replicationRemoval;
		private readonly IDictionary<string, JET_COLUMNID> replicationRemovalColumns;
		private readonly Session session;
		private readonly Transaction transaction;

		public PersistentHashTableActions(JET_INSTANCE instance,
		                                  string database,
		                                  Cache cache,
		                                  Guid instanceId)
		{
			this.cache = cache;
			this.instanceId = instanceId;
			session = new Session(instance);

			transaction = new Transaction(session);
			Api.JetOpenDatabase(session, database, null, out dbid, OpenDatabaseGrbit.None);
			keys = new Table(session, dbid, "keys", OpenTableGrbit.None);
			data = new Table(session, dbid, "data", OpenTableGrbit.None);
			identity = new Table(session, dbid, "identity_generator", OpenTableGrbit.None);
			list = new Table(session, dbid, "lists", OpenTableGrbit.None);
			replication = new Table(session, dbid, "replication_info", OpenTableGrbit.None);
			replicationRemoval = new Table(session, dbid, "replication_removal_info", OpenTableGrbit.None);
			keysColumns = Api.GetColumnDictionary(session, keys);
			dataColumns = Api.GetColumnDictionary(session, data);
			identityColumns = Api.GetColumnDictionary(session, identity);
			listColumns = Api.GetColumnDictionary(session, list);
			replicationColumns = Api.GetColumnDictionary(session, replication);
			replicationRemovalColumns = Api.GetColumnDictionary(session, replicationRemoval);
		}

		public JET_DBID DatabaseId
		{
			get { return dbid; }
		}

		public Session Session
		{
			get { return session; }
		}

		public Transaction Transaction
		{
			get { return transaction; }
		}

		public Table Keys
		{
			get { return keys; }
		}

		public Table Data
		{
			get { return data; }
		}

		public IDictionary<string, JET_COLUMNID> KeysColumns
		{
			get { return keysColumns; }
		}

		public IDictionary<string, JET_COLUMNID> DataColumns
		{
			get { return dataColumns; }
		}

		public void Dispose()
		{
			if (keys != null)
				keys.Dispose();
			if (data != null)
				data.Dispose();
			if (identity != null)
				identity.Dispose();
			if (list != null)
				list.Dispose();
			if (replication != null)
				replication.Dispose();
			if (replicationRemoval != null)
				replicationRemoval.Dispose();

			if (Equals(dbid, JET_DBID.Nil) == false)
				Api.JetCloseDatabase(session, dbid, CloseDatabaseGrbit.None);

			if (transaction != null)
				transaction.Dispose();

			if (session != null)
				session.Dispose();
		}

		public PutResult Put(PutRequest request)
		{
			var hash = GetSha256Hash(request);
			var versions = GatherActiveVersions(request.Key);
			if (versions.Length == 1)
			{
				// Test that the key doesn't already exist with matching value
				var values = Get(new GetRequest {Key = request.Key, SpecifiedVersion = versions[0]});
				if (values.Length == 1 && values[0].Sha256Hash.SequenceEqual(hash))
				{
					return new PutResult
					{
						ConflictExists = false,
						Version = versions[0]
					};
				}
			}

			// we always accept read only put requests, because
			// we assume that there can never be a conflict with them
			// since they are read only values.
			var doesAllVersionsMatch =
				request.IsReadOnly
					?
						request.IsReadOnly
					:
						DoesAllVersionsMatch(request.Key, request.ParentVersions);

			if (doesAllVersionsMatch == false &&
			    request.OptimisticConcurrency)
			{
				return new PutResult
				{
					ConflictExists = true,
					Version = null
				};
			}

			// always remove the active versions from the cache
			commitSyncronization.Add(() => cache.Remove(GetKey(request.Key)));
			if (doesAllVersionsMatch)
			{
				// we only remove existing versions from the 
				// cache if we delete them from the database
				foreach (var parentVersion in versions)
				{
					var copy = parentVersion;
					commitSyncronization.Add(() => cache.Remove(GetKey(request.Key, copy)));
				}
				DeleteAllKeyValuesForVersions(request.Key, versions);
			}

			var instanceIdForRow = instanceId;
			if (request.ReplicationVersion != null)
				instanceIdForRow = request.ReplicationVersion.InstanceId;

			var versionNumber = request.ReplicationVersion == null
			                    	?
			                    		GenerateVersionNumber()
			                    	:
			                    		request.ReplicationVersion.Number;
			using (var update = new Update(session, keys, JET_prep.Insert))
			{
				Api.SetColumn(session, keys, keysColumns["key"], request.Key, Encoding.Unicode);

				Api.SetColumn(session, keys, keysColumns["version_instance_id"], instanceIdForRow.ToByteArray());
				Api.SetColumn(session, keys, keysColumns["version_number"], versionNumber);

				if (request.Tag.HasValue)
					Api.SetColumn(session, keys, keysColumns["tag"], request.Tag.Value);

				if (request.ExpiresAt.HasValue)
					Api.SetColumn(session, keys, keysColumns["expiresAt"], request.ExpiresAt.Value.ToOADate());

				update.Save();
			}

			using (var update = new Update(session, data, JET_prep.Insert))
			{
				Api.SetColumn(session, data, dataColumns["key"], request.Key, Encoding.Unicode);
				Api.SetColumn(session, data, dataColumns["version_number"], versionNumber);
				Api.SetColumn(session, data, dataColumns["version_instance_id"], instanceIdForRow.ToByteArray());
				Api.SetColumn(session, data, dataColumns["data"], request.Bytes);
				Api.SetColumn(session, data, dataColumns["sha256_hash"], hash);
				Api.SetColumn(session, data, dataColumns["readonly"], request.IsReadOnly);

				var timestamp = DateTime.Now.ToOADate();

				if (request.Tag.HasValue)
					Api.SetColumn(session, data, dataColumns["tag"], request.Tag.Value);

				if (request.ReplicationTimeStamp.HasValue)
					timestamp = request.ReplicationTimeStamp.Value.ToOADate();
				Api.SetColumn(session, data, dataColumns["timestamp"], timestamp);

				if (request.ExpiresAt.HasValue)
					Api.SetColumn(session, data, dataColumns["expiresAt"], request.ExpiresAt.Value.ToOADate());

				WriteAllParentVersions(request.ParentVersions, data, dataColumns);

				update.Save();
			}

			return new PutResult
			{
				ConflictExists = doesAllVersionsMatch == false,
				Version = new ValueVersion
				{
					InstanceId = instanceIdForRow,
					Number = versionNumber
				}
			};
		}

		private static byte[] GetSha256Hash(PutRequest request)
		{
			byte[] hash;
			using (var sha256 = SHA256.Create())
			{
				hash = sha256.ComputeHash(request.Bytes);
			}
			return hash;
		}

		// insert into identity values(default);
		// select @@identity into @new_identity
		// delete from identity where id = @new_identity
		private int GenerateVersionNumber()
		{
			var bookmark = new byte[256];
			int bookmarkSize;
			using (var update = new Update(session, identity, JET_prep.Insert))
			{
				// force identity generator
				update.Save(bookmark, bookmark.Length, out bookmarkSize);
			}
			Api.JetGotoBookmark(session, identity, bookmark, bookmarkSize);
			var version = Api.RetrieveColumnAsInt32(session, identity, identityColumns["id"]);
			if (Api.TryMovePrevious(session, identity))
				Api.JetDelete(session, identity);
			return version.Value;
		}

		private void WriteAllParentVersions(IEnumerable<ValueVersion> parentVersions,
		                                    Table table,
		                                    IDictionary<string, JET_COLUMNID> columns)
		{
			var index = 1;
			foreach (var parentVersion in parentVersions)
			{
				var buffer = new byte[20];
				var versionAsBytes = parentVersion.InstanceId.ToByteArray();
				Buffer.BlockCopy(versionAsBytes, 0, buffer, 0, 16);
				versionAsBytes = BitConverter.GetBytes(parentVersion.Number);
				Buffer.BlockCopy(versionAsBytes, 0, buffer, 16, 4);

				Api.JetSetColumn(session, table, columns["parentVersions"], buffer, buffer.Length, SetColumnGrbit.None,
				                 new JET_SETINFO
				                 {
				                 	itagSequence = index
				                 });
				index += 1;
			}
		}

		private bool DoesAllVersionsMatch(string key,
		                                  ValueVersion[] parentVersions)
		{
			var activeVersions = GatherActiveVersions(key);

			if (activeVersions.Length != parentVersions.Length)
				return false;

			activeVersions = activeVersions
				.OrderBy(x => x)
				.ToArray();

			parentVersions = parentVersions.OrderBy(x => x).ToArray();

			for (var i = 0; i < activeVersions.Length; i++)
			{
				if (activeVersions[i].Number != parentVersions[i].Number ||
				    activeVersions[i].InstanceId != parentVersions[i].InstanceId)
					return false;
			}
			return true;
		}

		public Value[] Get(GetRequest request)
		{
			var values = new List<Value>();
			var activeVersions =
				request.SpecifiedVersion == null
					?
						GatherActiveVersions(request.Key)
					:
						new[] {request.SpecifiedVersion};

			var foundAllInCache = true;
			foreach (var activeVersion in activeVersions)
			{
				var cachedValue = cache[GetKey(request.Key, activeVersion)] as Value;
				if (cachedValue == null ||
				    (cachedValue.ExpiresAt.HasValue &&
				     DateTime.Now < cachedValue.ExpiresAt.Value))
				{
					values.Clear();
					foundAllInCache = false;
					break;
				}
				values.Add(cachedValue);
			}
			if (foundAllInCache)
				return values.ToArray();

			ApplyToKeyAndActiveVersions(data, activeVersions, request.Key, version =>
			{
				var value = ReadValueFromDataTable(version, request.Key);

				if (value != null)
					values.Add(value);
				else // remove it from the cache if exists
					commitSyncronization.Add(() => cache.Remove(GetKey(request.Key, version)));
			});

			commitSyncronization.Add(delegate
			{
				foreach (var value in values)
				{
					cache[GetKey(value.Key, value.Version)] = value;
				}
				cache[GetKey(request.Key)] = activeVersions;
			});

			return values.ToArray();
		}


		private Value ReadValueFromDataTable(ValueVersion version,
		                                     string key)
		{
			var expiresAtBinary = Api.RetrieveColumnAsDouble(session, data, dataColumns["expiresAt"]);
			DateTime? expiresAt = null;
			if (expiresAtBinary.HasValue)
			{
				expiresAt = DateTime.FromOADate(expiresAtBinary.Value);
				if (DateTime.Now > expiresAt)
					return null;
			}
			return new Value
			{
				Version = version,
				Key = key,
				Timestamp = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, data, dataColumns["timestamp"]).Value),
				ParentVersions = GetParentVersions(data, dataColumns),
				Tag = Api.RetrieveColumnAsInt32(session, data, dataColumns["tag"]),
				Data = Api.RetrieveColumn(session, data, dataColumns["data"]),
				Sha256Hash = Api.RetrieveColumn(session, data, dataColumns["sha256_hash"]),
				ReadOnly = Api.RetrieveColumnAsBoolean(session, data, dataColumns["readonly"]).Value,
				ExpiresAt = expiresAt
			};
		}

		private ValueVersion[] GetParentVersions(Table table,
		                                         IDictionary<string, JET_COLUMNID> columns)
		{
			var versions = new List<ValueVersion>();

			var index = 1;
			var size = -1;
			while (size != 0)
			{
				var buffer = new byte[20];
				Api.JetRetrieveColumn(session, table, columns["parentVersions"], buffer, 20, out size,
				                      RetrieveColumnGrbit.None, new JET_RETINFO
				                      {
				                      	itagSequence = index
				                      });
				if (size == 0)
					break;
				index += 1;
				var guidBuffer = new byte[16];
				Buffer.BlockCopy(buffer, 0, guidBuffer, 0, 16);
				versions.Add(new ValueVersion
				{
					InstanceId = new Guid(guidBuffer),
					Number = BitConverter.ToInt32(buffer, 16)
				});
			}
			return versions.ToArray();
		}

		private string GetKey(string key,
		                      ValueVersion version)
		{
			return GetKey(key) + "#" +
			       version.InstanceId + "/" +
			       version.Number;
		}

		private string GetKey(string key)
		{
			return "rhino.pht [" + instanceId + "]: " + key;
		}

		public void Commit()
		{
			CleanExpiredValues();
			transaction.Commit(CommitTransactionGrbit.None);
			foreach (var action in commitSyncronization)
			{
				action();
			}
		}

		private void CleanExpiredValues()
		{
			Api.JetSetCurrentIndex(session, keys, "by_expiry");
			Api.MakeKey(session, keys, DateTime.Now.ToOADate(), MakeKeyGrbit.NewKey);

			if (Api.TrySeek(session, keys, SeekGrbit.SeekLT) == false)
				return;
			var count = 0;
			do
			{
				var key = Api.RetrieveColumnAsString(session, keys, keysColumns["key"], Encoding.Unicode);
				var version = ReadVersion(keys, keysColumns);

				Api.JetDelete(session, keys);

				ApplyToKeyAndActiveVersions(data, new[] {version}, key, v => Api.JetDelete(session, data));

				count += 1;
				// We need to break the unbounded result set here, because there may be many
				// rows that are not valid, so we break after a hundred or so making sure that 
				// the commit size doesn't get too big.	This will get cleaned up by the next
				// commit any way.
				if (count > 100)
					break;
			} while (Api.TryMovePrevious(session, keys));
		}

		private ValueVersion ReadVersion(Table table,
		                                 IDictionary<string, JET_COLUMNID> columns)
		{
			var versionNumber = Api.RetrieveColumnAsInt32(session, table, columns["version_number"]).Value;
			var versionInstanceId = Api.RetrieveColumn(session, table, columns["version_instance_id"]);

			return new ValueVersion
			{
				InstanceId = new Guid(versionInstanceId),
				Number = versionNumber
			};
		}

		// delete from keys
		// where key = key and version in (versions)
		//
		// delete from data
		// where key = key and version in (versions)
		//
		// delete from replicatio
		// where key = key and version in (versions)
		private void DeleteAllKeyValuesForVersions(string key,
		                                           IEnumerable<ValueVersion> versions)
		{
			ApplyToKeyAndActiveVersions(keys, versions, key,
			                            version => Api.JetDelete(session, keys));

			ApplyToKeyAndActiveVersions(data, versions, key, version =>
			                                                 Api.JetDelete(session, data));

			ApplyToKeyAndActiveVersions(replication, versions, key, 
				version => Api.JetDelete(session, replication));
		}

		// select * from :table 
		// where pk.0 = :key and pk.1 = :version.number
		// and pk.3 = :version.instanceId
		private void ApplyToKeyAndActiveVersions(Table table,
		                                         IEnumerable<ValueVersion> versions,
		                                         string key,
		                                         Action<ValueVersion> action)
		{
			Api.JetSetCurrentIndex(session, table, "pk");
			foreach (var version in versions)
			{
				Api.MakeKey(session, table, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
				Api.MakeKey(session, table, version.Number, MakeKeyGrbit.None);
				Api.MakeKey(session, table, version.InstanceId.ToByteArray(), MakeKeyGrbit.None);

				if (Api.TrySeek(session, table, SeekGrbit.SeekEQ) == false)
					continue;

				action(version);
			}
		}

		// select new ValueVersion(version_number, version_instance_id) 
		// from keys where key = :key
		private ValueVersion[] GatherActiveVersions(string key)
		{
			var cachedActiveVersions = cache[GetKey(key)];
			if (cachedActiveVersions != null)
				return (ValueVersion[]) cachedActiveVersions;

			Api.JetSetCurrentIndex(session, keys, "by_key");
			Api.MakeKey(session, keys, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			var exists = Api.TrySeek(session, keys, SeekGrbit.SeekEQ);
			if (exists == false)
				return new ValueVersion[0];

			Api.MakeKey(session, keys, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, keys,
			                     SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			var ids = new List<ValueVersion>();
			do
			{
				var version = ReadVersion(keys, keysColumns);

				ids.Add(version);
			} while (Api.TryMoveNext(session, keys));
			return ids.ToArray();
		}

		public bool Remove(RemoveRequest request)
		{
			ValueVersion[] versions;
			bool doesAllVersionsMatch;
			if (request.SpecificVersion != null)
			{
				versions = new[] { request.SpecificVersion };
				doesAllVersionsMatch = DoesAllVersionsMatch(request.Key, versions);
			}
			else
			{
				versions = GatherActiveVersions(request.Key);
				doesAllVersionsMatch = true;
			}
			if (doesAllVersionsMatch)
			{
				DeleteAllKeyValuesForVersions(request.Key, versions);

				foreach (var version in versions)
				{
					var copy = version;
					commitSyncronization.Add(() => cache.Remove(GetKey(request.Key, copy)));
				}
				commitSyncronization.Add(() => cache.Remove(GetKey(request.Key)));
			}
			return doesAllVersionsMatch;
		}

		public int AddItem(AddItemRequest request)
		{
			var bookmark = new byte[256];
			int actualBookmarkSize;
			using (var update = new Update(Session, list, JET_prep.Insert))
			{
				Api.SetColumn(session, list, listColumns["key"], request.Key, Encoding.Unicode);
				Api.SetColumn(session, list, listColumns["data"], request.Data);

				update.Save(bookmark, bookmark.Length, out actualBookmarkSize);
			}

			Api.JetGotoBookmark(session, list, bookmark, actualBookmarkSize);
			return (int) Api.RetrieveColumnAsInt32(session, list, listColumns["id"]);
		}

		public KeyValuePair<int, byte[]>[] GetItems(GetItemsRequest request)
		{
			Api.JetSetCurrentIndex(session, list, "by_key");
			Api.MakeKey(session, list, request.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, list, SeekGrbit.SeekEQ) == false)
				return new KeyValuePair<int, byte[]>[0];

			Api.MakeKey(session, list, request.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, list,
			                     SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			var results = new List<KeyValuePair<int, byte[]>>();
			do
			{
				var id = Api.RetrieveColumnAsInt32(session, list, listColumns["id"]);
				var bytes = Api.RetrieveColumn(session, list, listColumns["data"]);
				results.Add(new KeyValuePair<int, byte[]>(id.Value, bytes));
			} while (Api.TryMoveNext(Session, list));

			return results.ToArray();
		}

		public void RemoveItem(RemoveItemRequest request)
		{
			Api.JetSetCurrentIndex(session, list, "pk");
			Api.MakeKey(session, list, request.Key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, list, request.Id, MakeKeyGrbit.None);
			if (Api.TrySeek(session, list, SeekGrbit.SeekEQ))
				Api.JetDelete(session, list);
		}

		public bool HasTag(int tag)
		{
			Api.JetSetCurrentIndex(session, keys, "by_tag");
			Api.MakeKey(session, keys, tag, MakeKeyGrbit.NewKey);
			return Api.TrySeek(session, keys, SeekGrbit.SeekEQ);
		}

		public IEnumerable<GetRequest> GetKeysForTag(int tag)
		{
			Api.JetSetCurrentIndex(session, keys, "by_tag");
			Api.MakeKey(session, keys, tag, MakeKeyGrbit.NewKey);
			if (Api.TrySeek(session, keys, SeekGrbit.SeekEQ) == false)
				yield break;

			Api.MakeKey(session, keys, tag, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, keys,
			                     SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			do
			{
				yield return new GetRequest
				{
					Key = Api.RetrieveColumnAsString(session, keys, keysColumns["key"]),
					SpecifiedVersion = ReadVersion(keys, keysColumns)
				};
			} while (Api.TryMoveNext(session, keys));
		}

		public bool HasReplicationInfo(string key,
		                               ValueVersion version,
		                               byte[] hash)
		{
			Api.JetSetCurrentIndex(session, replication, "pk");
			Api.MakeKey(session, replication, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, replication, version.Number, MakeKeyGrbit.None);
			Api.MakeKey(session, replication, version.InstanceId.ToByteArray(), MakeKeyGrbit.None);
			Api.MakeKey(session, replication, hash, MakeKeyGrbit.None);

			return Api.TrySeek(session, replication, SeekGrbit.SeekEQ);
		}


		public void AddReplicationInfo(string key,
		                               ValueVersion version,
		                               byte[] hash)
		{
			using (var update = new Update(session, replication, JET_prep.Insert))
			{
				Api.SetColumn(session, replication, replicationColumns["key"], key, Encoding.Unicode);

				Api.SetColumn(session, replication, replicationColumns["version_number"], version.Number);
				Api.SetColumn(session, replication, replicationColumns["version_instance_id"], version.InstanceId.ToByteArray());

				Api.SetColumn(session, replication, replicationColumns["sha256_hash"], hash);

				update.Save();
			}
		}

		public void AddReplicationRemovalInfo(string key,
									   ValueVersion version,
									   byte[] hash)
		{
			using (var update = new Update(session, replicationRemoval, JET_prep.Insert))
			{
				Api.SetColumn(session, replicationRemoval, replicationRemovalColumns["key"], key, Encoding.Unicode);

				Api.SetColumn(session, replicationRemoval, replicationRemovalColumns["version_number"], version.Number);
				Api.SetColumn(session, replicationRemoval, replicationRemovalColumns["version_instance_id"], version.InstanceId.ToByteArray());

				Api.SetColumn(session, replicationRemoval, replicationRemovalColumns["sha256_hash"], hash);

				update.Save();
			}
		}

		public IEnumerable<RemoveRequest> ConsumeRemovalReplicationInfo(byte[] hash)
		{
			Api.JetSetCurrentIndex(session, replicationRemoval, "by_hash");
			Api.MakeKey(session, replicationRemoval, hash, MakeKeyGrbit.NewKey);

			if (Api.TrySeek(session, replicationRemoval, SeekGrbit.SeekEQ) == false)
				yield break;

			Api.MakeKey(session, replicationRemoval, hash, MakeKeyGrbit.NewKey);
			Api.JetSetIndexRange(session, replicationRemoval,
				SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			do
			{
				var request = new RemoveRequest
				{
					Key = Api.RetrieveColumnAsString(session, replicationRemoval, replicationColumns["key"]),
					SpecificVersion = ReadVersion(replicationRemoval, replicationRemovalColumns)
				};
				Api.JetDelete(session, replicationRemoval);
				yield return request;
			} while (Api.TryMoveNext(session, replicationRemoval));
		}

		public IEnumerable<byte[]> GetReplicationHashes(string key,
		                                        ValueVersion version)
		{
			Api.JetSetCurrentIndex(session, replication, "by_key_and_version");
			Api.MakeKey(session, replication, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, replication, version.Number, MakeKeyGrbit.None);
			Api.MakeKey(session, replication, version.InstanceId.ToByteArray(), MakeKeyGrbit.None);

			if (Api.TrySeek(session, replication, SeekGrbit.SeekEQ) == false)
				yield break;

			Api.MakeKey(session, replication, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
			Api.MakeKey(session, replication, version.Number, MakeKeyGrbit.None);
			Api.MakeKey(session, replication, version.InstanceId.ToByteArray(), MakeKeyGrbit.None);

			Api.JetSetIndexRange(session, replication,
			                     SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);

			do
			{
				yield return Api.RetrieveColumn(session, replication, replicationColumns["sha256_hash"]);
			} while (Api.TryMoveNext(session, replication));

		}
	}
}