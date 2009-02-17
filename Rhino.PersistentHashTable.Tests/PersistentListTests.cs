namespace Rhino.PersistentHashTable.Tests
{
	using System;
	using Xunit;

	public class PersistentListTests : PersistentTestBase
	{
		[Fact]
		public void Can_add_item_to_list()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					var id = actions.AddItem(new AddItemRequest
					{
						Data = new byte[] { 1, 2, 4 },
						Key = "test"
					});
				});
			}
		}

		[Fact]
		public void Can_list_items()
		{
			int item2 = 0;
			int item1 = 0;
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					item1 = actions.AddItem(new AddItemRequest
					{
						Data = new byte[] { 1, 2, 4 },
						Key = "test"
					});
					item2 = actions.AddItem(new AddItemRequest
					{
						Data = new byte[] { 5, 3, 1 },
						Key = "test"
					});

					actions.Commit();
				});


				table.Batch(actions =>
				{
					var items = actions.GetItems(new GetItemsRequest
					{
						Key = "test"
					});
					Assert.Equal(item1, items[0].Key);
					Assert.Equal(item2, items[1].Key);

					Assert.Equal(new byte[] { 1, 2, 4 }, items[0].Value);
					Assert.Equal(new byte[] { 5, 3, 1 }, items[1].Value);
				});
			}
		}

		[Fact]
		public void Can_remove_items()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					int item1 = actions.AddItem(new AddItemRequest
					{
						Data = new byte[] { 1, 2, 4 },
						Key = "test"
					});
					actions.AddItem(new AddItemRequest
					{
						Data = new byte[] { 3, 6, 4 },
						Key = "test"
					});
					 actions.RemoveItem(new RemoveItemRequest
					{
                        Id = item1,
						Key = "test"
					});

					actions.Commit();
				});


				table.Batch(actions =>
				{
					var items = actions.GetItems(new GetItemsRequest
					{
						Key = "test"
					});
					Assert.Equal(1, items.Length);
				});
			}
		}

		[Fact]
		public void will_get_items_from_current_list_only()
		{
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();

				table.Batch(actions =>
				{
					actions.AddItem(new AddItemRequest
					{
						Data = new byte[] { 1, 2, 4 },
						Key = "test"
					});
					actions.AddItem(new AddItemRequest
					{
						Data = new byte[] { 5, 3, 1 },
						Key = "test"
					});
					actions.AddItem(new AddItemRequest
					{
						Data = new byte[] { 5, 3, 1 },
						Key = "test2"
					});
					actions.Commit();
				});


				table.Batch(actions =>
				{
					var items = actions.GetItems(new GetItemsRequest
					{
						Key = "test"
					});
					Assert.Equal(2, items.Length);
				});
			}
		}
	}
}