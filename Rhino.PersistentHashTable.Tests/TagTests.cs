using System.Linq;
using Xunit;

namespace Rhino.PersistentHashTable.Tests
{
	public class TagTests : PersistentTestBase
	{
		[Fact]
		public void Can_put_with_tag()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Bytes = new byte[]{1,2,3,4},
						Key = "test",
                        Tag = 1
					});

					actions.Commit();
				});

				table.Batch(actions =>
				{
					var values = actions.Get(new GetRequest()
					{
						Key = "test",
					});

					Assert.Equal(1, values[0].Tag);

					actions.Commit();
				});
			}
		}

		[Fact]
		public void Can_check_existance_of_tag()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Bytes = new byte[] { 1, 2, 3, 4 },
						Key = "test",
						Tag = 1
					});

					actions.Commit();
				});

				table.Batch(actions =>
				{
					Assert.True(actions.HasTag(1));
					Assert.False(actions.HasTag(2));

					actions.Commit();
				});
			}
		}

		[Fact]
		public void Can_get_all_rows_with_tag()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.Put(new PutRequest
					{
						Bytes = new byte[] { 1, 2, 3, 4 },
						Key = "test",
						Tag = 1
					});

					actions.Put(new PutRequest
					{
						Bytes = new byte[] { 1, 2, 3, 4 },
						Key = "test2",
						Tag = 1
					});

					actions.Put(new PutRequest
					{
						Bytes = new byte[] { 1, 2, 3, 4 },
						Key = "test3",
						Tag = 2
					});

					actions.Commit();
				});

				table.Batch(actions =>
				{
					var results = actions.GetKeysForTag(1).ToArray();
					Assert.Equal(2, results.Length);
					Assert.Equal("test", results[0].Key);
					Assert.Equal("test2", results[1].Key);

					results = actions.GetKeysForTag(2).ToArray();
					Assert.Equal(1, results.Length);
					Assert.Equal("test3", results[0].Key);

					actions.Commit();
				});
			}
		}
	}
}
