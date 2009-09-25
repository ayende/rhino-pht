namespace Rhino.PersistentHashTable
{
	public interface IVersionGenerator
	{
		int GenerateNextVersion();
	}
}