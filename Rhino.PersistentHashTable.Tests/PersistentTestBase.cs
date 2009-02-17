namespace Rhino.PersistentHashTable.Tests
{
	using System.IO;

	public class PersistentTestBase
	{
		protected const string testDatabase = "test.esent";

		public PersistentTestBase()
		{
			if (Directory.Exists(testDatabase))
				Directory.Delete(testDatabase, true);
		}
	}
}