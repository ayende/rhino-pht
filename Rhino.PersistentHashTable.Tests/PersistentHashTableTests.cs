using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Microsoft.Isam.Esent.Interop;
using Rhino.PersistentHashTable.Actions;
using Xunit;

namespace Rhino.PersistentHashTable.Tests
{
	public class PersistentHashTableTests : PersistentTestBase
	{
		[Fact]
		public void Can_get_timestamp_from_pht()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Bytes = new byte[] { 1, 2, },
						Key = "test",
						ParentVersions = new ValueVersion[0]
					});


					actions.Commit();
				});

				table.Batch(actions =>
				{
					Value[] values = actions.Get(new GetRequest { Key = "test" });

					Assert.NotEqual(DateTime.MinValue, values[0].Timestamp);

					actions.Commit();
				});
			}
		}

		[Fact]
		public void will_generate_sha256_for_new_values()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Bytes = new byte[] { 1, 2, },
						Key = "test",
						ParentVersions = new ValueVersion[0]
					});


					actions.Commit();
				});

				table.Batch(actions =>
				{
					Value[] values = actions.Get(new GetRequest { Key = "test" });

					Assert.NotNull(values[0].Sha256Hash);
					Assert.NotEmpty(values[0].Sha256Hash);
					actions.Commit();
				});
			}
		}


		[Fact]
		public void Id_of_table_is_persistent()
		{
			Guid id;
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				id = table.Id;
				Assert.NotEqual(Guid.Empty, id);
			}

			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				Assert.NotEqual(Guid.Empty, table.Id);
				Assert.Equal(id, table.Id);
			}
		}

		[Fact]
		public void Can_set_the_replication_version_and_get_it_back()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();
				Guid guid = Guid.NewGuid();
				table.Batch(actions =>
				{
					PutResult result = actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 3 },
						ReplicationVersion = new ValueVersion
						{
							InstanceId = guid,
							Number = 53
						}
					});

					Assert.Equal(53, result.Version.Number);
					Assert.Equal(guid, result.Version.InstanceId);

					actions.Commit();
				});
			}
		}

		[Fact]
		public void Can_save_and_load_item_from_cache()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					Value[] values = actions.Get(new GetRequest { Key = "test" });
					Assert.Equal(1, values[0].Version.Number);
					Assert.Equal(new byte[] { 1 }, values[0].Data);
				});
			}
		}

		[Fact]
		public void Can_insert_same_value_to_pht_in_two_seperate_instances()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "test",
						Bytes = new byte[] { 1 }
					});

					actions.Commit();
				});
			}

			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "test",
						Bytes = new byte[] { 1 }
					});

					actions.Commit();
				});
			}
		}

		[Fact]
		public void Can_get_hash_from_cache()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					byte[] bytes = Encoding.UTF8.GetBytes("abcdefgiklmnqrstwxyz");
					actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = bytes
					});
					Value[] values = actions.Get(new GetRequest
					{
						Key = "test"
					});
					Assert.Equal(
						SHA256.Create().ComputeHash(bytes),
						values[0].Sha256Hash
						);
				});
			}
		}

		[Fact]
		public void Can_remove_item_from_table_when_there_is_more_than_single_item()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				ValueVersion versionOfA = null, versionOfC = null;

				table.Batch(actions =>
				{
					versionOfA = actions.Put(
						new PutRequest
						{
							Key = "a",
							ParentVersions = new ValueVersion[0],
							Bytes = new byte[] { 1 }
						}
						).Version;
					actions.Put(new PutRequest
					{
						Key = "b",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					versionOfC = actions.Put(
						new PutRequest
						{
							Key = "c",
							ParentVersions = new ValueVersion[0],
							Bytes = new byte[] { 1 }
						}).Version;
					actions.Put(
						new PutRequest
						{
							Key = "d",
							ParentVersions = new ValueVersion[0],
							Bytes = new byte[] { 1 }
						});

					actions.Commit();
				});

				table.Batch(actions =>
				{
					bool removed = actions.Remove(new RemoveRequest
					{
						Key = "a",
						ParentVersions = new[] { versionOfA }
					});
					Assert.True(removed);
					removed = actions.Remove(new RemoveRequest
					{
						Key = "c",
						ParentVersions = new[] { versionOfC }
					});
					Assert.True(removed);
					actions.Commit();
				});
			}
		}

		[Fact]
		public void Can_get_item_in_specific_version()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					PutResult version1 = actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 2 }
					});
					Value[] value = actions.Get(new GetRequest
					{
						Key = "test",
						SpecifiedVersion = version1.Version
					});
					Assert.Equal(new byte[] { 1 }, value[0].Data);
				});
			}
		}

		[Fact]
		public void After_resolving_conflict_will_remove_old_version_of_data()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					PutResult version1 = actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					PutResult version2 = actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 2 }
					});
					Value[] value = actions.Get(new GetRequest
					{
						Key = "test",
						SpecifiedVersion = version1.Version
					});
					Assert.Equal(new byte[] { 1 }, value[0].Data);

					actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new[]
                        {
                            version1.Version,
                            version2.Version
                        },
						Bytes = new byte[] { 3 }
					});

					actions.Commit();

					Assert.Empty(actions.Get(new GetRequest
					{
						Key = "test",
						SpecifiedVersion = version1.Version
					}));
				});
			}
		}

		[Fact]
		public void Can_use_optimistic_concurrency()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					PutResult put = actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 2 },
						OptimisticConcurrency = true
					});
					Assert.True(put.ConflictExists);

					actions.Commit();

					Assert.Equal(1, actions.Get(new GetRequest
					{
						Key = "test"
					}).Length);
				});
			}
		}


		[Fact]
		public void Save_several_items_for_same_version_will_save_all_of_them()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 2 }
					});
					Value[] values = actions.Get(new GetRequest
					{
						Key = "test"
					});
					Assert.Equal(2, values.Length);

					Assert.Equal(1, values[0].Version.Number);
					Assert.Equal(new byte[] { 1 }, values[0].Data);

					Assert.Equal(2, values[1].Version.Number);
					Assert.Equal(new byte[] { 2 }, values[1].Data);
				});
			}
		}

		[Fact]
		public void Writing_identical_data_to_existing_vlaue_will_not_create_conflict()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					PutResult put1 = actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					PutResult put2 = actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					Value[] values = actions.Get(new GetRequest { Key = "test" });
					Assert.Equal(1, values.Length);
					Assert.Equal(put1.Version.Number, put2.Version.Number);
					Assert.Equal(put1.Version.InstanceId, put2.Version.InstanceId);
				});
			}
		}

		[Fact]
		public void Can_resolve_conflict()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					PutResult version1 = actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					PutResult version2 = actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 2 }
					});
					Value[] values = actions.Get(new GetRequest { Key = "test" });
					Assert.Equal(2, values.Length);
					actions.Put(new PutRequest
					{
						Key = "test",
						ParentVersions = new[] { version1.Version, version2.Version },
						Bytes = new byte[] { 3 }
					});

					values = actions.Get(new GetRequest { Key = "test" });
					Assert.Equal(1, values.Length);
					Assert.Equal(new byte[] { 3 }, values[0].Data);
					Assert.Equal(3, values[0].Version.Number);
				});
			}
		}

		[Fact]
		public void Cannot_query_with_partial_item_key()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});
					Value[] values = actions.Get(new GetRequest { Key = "abc10" });
					Assert.Equal(0, values.Length);

					values = actions.Get(new GetRequest { Key = "abc1" });
					Assert.NotEqual(0, values.Length);
				});
			}
		}

		[Fact]
		public void Can_query_for_item_with_expiry()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 },
						ExpiresAt = DateTime.Now.AddYears(1)
					});

					Value[] values = actions.Get(new GetRequest { Key = "abc1" });
					Assert.NotEqual(0, values.Length);
				});
			}
		}

		[Fact]
		public void Can_query_for_item_history()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					PutResult version1 = actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 6 }
					});

					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new[] { version1.Version },
						Bytes = new byte[] { 1 }
					});
					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new[] { version1.Version },
						Bytes = new byte[] { 2 }
					});
					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new[]
                        {
                            new ValueVersion
                            {
                                InstanceId = version1.Version.InstanceId,
                                Number = 3
                            },
                        },
						Bytes = new byte[] { 3 }
					});

					Value[] values = actions.Get(new GetRequest { Key = "abc1" });
					Assert.Equal(3, values.Length);

					Assert.Equal(new[] { 1 }, values[0].ParentVersions.Select(x => x.Number).ToArray());
					Assert.Equal(new[] { 1 }, values[1].ParentVersions.Select(x => x.Number).ToArray());
					Assert.Equal(new[] { 3 }, values[2].ParentVersions.Select(x => x.Number).ToArray());
				});
			}
		}

		[Fact]
		public void When_setting_the_same_value_on_the_key_twice_will_not_create_new_version()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					PutResult version1 = actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 6 }
					});

					var version2 = actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new[] { version1.Version },
						Bytes = new byte[] { 6 }
					});

					Assert.False(version2.ConflictExists);
					Assert.Equal(version1.Version.InstanceId, version2.Version.InstanceId);
					Assert.Equal(version1.Version.Number, version2.Version.Number);
				});
			}
		}

		[Fact]
		public void After_item_expires_it_cannot_be_retrieved()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 },
						ExpiresAt = DateTime.Now.AddYears(-1),
						OptimisticConcurrency = false
					});
					Value[] values = actions.Get(new GetRequest { Key = "abc1" });

					Assert.Equal(0, values.Length);
				});
			}
		}

		[Fact]
		public void Can_put_a_value_as_read_only()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 },
						OptimisticConcurrency = false,
                        IsReadOnly = true
					});
					Value[] values = actions.Get(new GetRequest { Key = "abc1" });

					Assert.True(values[0].ReadOnly);
				});
			}
		}

		[Fact]
		public void Can_overwrite_a_read_only_with_no_conflicts()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 },
						OptimisticConcurrency = false,
						IsReadOnly = true
					});

					var result = actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 2 },
						OptimisticConcurrency = false,
						IsReadOnly = true
					});
					Assert.False(result.ConflictExists);

					Value[] values = actions.Get(new GetRequest { Key = "abc1" });

					Assert.True(values[0].ReadOnly);
					Assert.Equal(1, values.Length);
					Assert.Equal(new byte[]{2}, values[0].Data);
				});
			}
		}

		[Fact]
		public void After_item_expires_it_will_be_removed_on_next_commit()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 },
						ExpiresAt = DateTime.Now,
					});

					actions.Commit();
				});

				table.Batch(actions => actions.Commit());

				int numRecords = -1;
				table.Batch(actions =>
				{
					Api.JetSetCurrentIndex(actions.Session, actions.Keys, null); //primary
					Api.JetIndexRecordCount(actions.Session, actions.Keys, out numRecords, 0);
				});

				Assert.Equal(0, numRecords);
			}
		}

		[Fact]
		public void ExportToCsv_exports_database_table_contents_to_csv_file_to_specified_path()
		{
			var dbRootPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, testDatabase);
			var dbFilePath = Path.Combine(dbRootPath, Path.GetFileName(testDatabase));
			var exportPath = Path.Combine(dbRootPath, "export");

			//Clear previous test results.
			if (Directory.Exists(exportPath))
				Directory.Delete(exportPath);
			Directory.CreateDirectory(exportPath);

			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();
				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "abc1",
						Bytes = new byte[] { 1 }
					});
					actions.Put(new PutRequest
					{
						Key = "abc2",
						Bytes = new byte[] { 2 }
					});
					actions.Commit();
				});
			}

			var export = new ExportToCsv(dbFilePath, exportPath);
			export.Execute();

			Assert.True(Directory.Exists(exportPath));
			Assert.True(File.Exists(Path.Combine(exportPath, "keys.csv")));
			Assert.True(File.Exists(Path.Combine(exportPath, "data.csv")));
			Assert.True(File.Exists(Path.Combine(exportPath, "details.csv")));
			Assert.True(File.Exists(Path.Combine(exportPath, "lists.csv")));

			var linesInKeys = File.ReadAllLines(Path.Combine(exportPath, "keys.csv"));
			var linesInData = File.ReadAllLines(Path.Combine(exportPath, "data.csv"));
			Assert.True(linesInKeys.Length > 0);
			Assert.True(linesInData.Length > 0);
		}

		[Fact]
		public void ExpireDataFromPhtByTimestamp_removes_data_whose_expiration_is_lesser_or_equal_than_specified()
		{
			var dbRootPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, testDatabase);
			var dbFilePath = Path.Combine(dbRootPath, Path.GetFileName(testDatabase));

			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();
				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "expire1",
						Bytes = new byte[] { 1 },
						ReplicationTimeStamp = DateTime.Now.AddHours(-1)
					});
					var parent = actions.Put(new PutRequest
					{
						Key = "expire2",
						Bytes = new byte[] { 2 },
						ReplicationTimeStamp = DateTime.Now.AddMinutes(-20)
					});

					actions.Put(new PutRequest
					{
						Key = "expire2",
						Bytes = new byte[] { 2, 1 },
						ParentVersions = new[] { parent.Version },
						ReplicationTimeStamp = DateTime.Now.AddMinutes(-10)
					});
					actions.Put(new PutRequest
					{
						Key = "shouldnotexpire",
						Bytes = new byte[] { 3 },
						ReplicationTimeStamp = DateTime.Now.AddMinutes(30)
					});
					actions.Commit();
				});
			}

			var expiryUtil = new ExpireDataFromPhtByTimestamp(dbFilePath, DateTime.Now);
			expiryUtil.Excute();

			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					var results1 = actions.Get(new GetRequest
					{
						Key = "expire1",
						SpecifiedVersion = null
					});
					Assert.True(results1.Length == 0);

					var results2 = actions.Get(new GetRequest
					{
						Key = "expire2",
						SpecifiedVersion = null
					});
					Assert.True(results2.Length == 0);

					var results3 = actions.Get(new GetRequest
					{
						Key = "shouldnotexpire",
						SpecifiedVersion = null
					});
					Assert.True(results3.Length > 0);
				});
			}
		}

		[Fact]
		public void Interleaved()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Key = "abc1",
						ParentVersions = new ValueVersion[0],
						Bytes = new byte[] { 1 }
					});


					table.Batch(actions2 =>
					{
						actions2.Put(new PutRequest
						{
							Key = "dve",
							ParentVersions = new ValueVersion[0],
							Bytes = new byte[] { 3 }
						});
						actions2.Commit();
					});

					actions.Commit();
				});

				table.Batch(actions =>
				{
					Assert.NotEmpty(actions.Get(new GetRequest { Key = "abc1" }));
					Assert.NotEmpty(actions.Get(new GetRequest { Key = "dve" }));
				});
			}
		}
	}
}