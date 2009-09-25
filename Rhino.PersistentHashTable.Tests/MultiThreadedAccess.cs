using System;
using System.Collections.Generic;
using System.Threading;
using Xunit;

namespace Rhino.PersistentHashTable.Tests
{
	public class MultiThreadedAccess : PersistentTestBase
	{
		[Fact]
		public void CanPutDifferentKeysIntoPHTConcurrently()
		{
			var exceptions = new List<Exception>();
			using (var table = new PersistentHashTable(testDatabase))
			{
				table.Initialize();
				int count = 0;

				const int threadCount = 50;
				for (int i = 0; i < threadCount; i++)
				{
					new Thread(state =>
					{
						var j = (int) state;

						try
						{
							for (int k = 0; k < 10; k++)
							{
								table.Batch(actions =>
								{
									actions.Put(new PutRequest
									{
										Key = k + "-" + j.ToString(),
										Bytes = new byte[] { 4, 3, 5 }
									});

									actions.Commit();
								});
							}
						}
						catch (Exception e)
						{
							lock(exceptions)
								exceptions.Add(e);
						}
						finally
						{
							Interlocked.Increment(ref count);
						}
					},i)
					{
						IsBackground = true
					}.Start(i);
				}

				while(Thread.VolatileRead(ref count) != threadCount)
				{
					Thread.Sleep(100);
				}

				Assert.Empty(exceptions);
			}
		}
	}
}